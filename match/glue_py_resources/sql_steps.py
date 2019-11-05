from utility_functions import sql_gen_concat_cols, sql_gen_single_blocking_rule, sql_gen_col_selection_compare_cols

from pyspark.sql import functions as f
from pyspark.ml.feature import RegexTokenizer, VectorAssembler

def sql_concat_cols(df, spark, cols):

    concat_expr = sql_gen_concat_cols(cols)
    df.registerTempTable('df')

    sql = f"""
    select *, {concat_expr} as concat
    from df
    """
    return spark.sql(sql)

def sql_add_unique_row_id_to_original_table(df, spark):
    """
    Add a row id

    Note that monotonically_increasing_id doen't guarantee ascending integers.  This means it's unreliable as a joining key.
    i.e. a BAD idea
    https://stackoverflow.com/questions/48209667/using-monotonically-increasing-id-for-assigning-row-number-to-pyspark-datafram
    """
    concat_expr = sql_gen_concat_cols(df.columns)
    return df.withColumn("row_id", f.expr(f"substr(sha2({concat_expr},256),0,12)"))

def tokenise_concat_field(df, spark):
    """
    Take the 'concat' field, which contains the whole record as a single string
    and split it into an array of tokens
    """
    tokenizer = RegexTokenizer(
        inputCol="concat", outputCol="tokens", pattern="[\s\-\.@]")
    df = tokenizer.transform(df)
    # df = df.drop('concat')  # Needed later for overall edit distance!
    return df


def get_token_probabilities_lookup(df, spark):
    """
    Get a lookup table like
    +-----+-----+
    |token| prob|
    +-----+-----+
    |   ab|0.082|
    +-----+-----+
    """

    counts_lookup = df.select(
        f.explode('tokens').alias('token')).groupBy('token').count()

    counts_lookup.registerTempTable("counts_lookup")

    sql = """
    select token, count/(select sum(count) from counts_lookup) as prob
    from counts_lookup
    """
    probs_df = spark.sql(sql)

    return probs_df


def get_potential_matches(df, spark, list_of_rules):
    """
    Use a series of rules to create a table of potential matches

    example of list of rules:
    [
        'l.first_name = r.first_name and l.surname = r.surname',
        'l.first_name = r.first_name and l.city = r.city'
    ]

    """

    tables_to_union_sql = [sql_gen_single_blocking_rule(r) for r in list_of_rules]
    unions = "\n union all \n".join(tables_to_union_sql)

    df.registerTempTable('df')

    sql = f"""
    select row_id_l, row_id_r, tokens_l, tokens_r,
    concat_ws('_', row_id_l, row_id_r) as match_id
    from
    (
        {unions}
    )
    """


    all_combinations = spark.sql(sql)
    unique_combinations = all_combinations.dropDuplicates(["row_id_l", "row_id_r"])

    return unique_combinations


def get_word_combinations(df, spark):
    """
    From list of potential matches, get table comparing each token
    to determine which tokens match
    """

    df.registerTempTable('df')

    sql = """
    select posexplode_outer(tokens_l) as (index_l, token_l), row_id_l, match_id as match_id_l
    from df
    """
    l = spark.sql(sql)
    l.registerTempTable('l')

    sql = """
    select posexplode_outer(tokens_r) as (index_r, token_r), row_id_r, match_id as match_id_r
    from df
    """
    r = spark.sql(sql)
    r.registerTempTable('r')

    sql = """

    select * from l
    cross join r
    on l.match_id_l = r.match_id_r
    and index_l <= index_r
    and row_id_l < row_id_r
    """

    return spark.sql(sql)


def find_exact_and_fuzzy_matching_tokens(df, spark):
    """
    Find matching tokens
    """

    df.registerTempTable('df')
    sql = """
    select *, token_l == token_r as exact_match
    from df
    where
    token_l == token_r
    or
    soundex(token_l) == soundex(token_r)
    or
    levenshtein(token_l, token_r)/array_max(array(length(token_l), length(token_r))) < 0.1
    """

    return spark.sql(sql)

def matching_tokens_with_prob(matching_tokens, prob_lookup, spark):

    matching_tokens.registerTempTable('matching_tokens')
    prob_lookup.registerTempTable('prob_lookup')

    sql = """
    select w.exact_match, w.token_l, row_id_l, w.token_r, row_id_r,
    case
    when exact_match then ln(array_max(array(p_r.prob, p_l.prob)))
    else
    ln(array_max(array(p_r.prob, p_l.prob))) * 0.5
    end
    as log_prob

    from matching_tokens as w
    left join prob_lookup as p_l
    on w.token_l == p_l.token
    left join prob_lookup as p_r
    on w.token_r = p_r.token


    """
    log_probs = spark.sql(sql)
    return log_probs


def reduce_probability_tokens_to_records(matching_tokens, spark):
    # Rollup to single probability per row_id_l, row_id_r
    matching_tokens.registerTempTable('matching_tokens')

    # TODO: Pretty sure this juse needs a sum(log_prob) not the aggregate function
    # This is a hangover from when I was taking the union of arrays

    sql = """
    select
        row_id_l,
        row_id_r,
        aggregate(collect_list(log_prob), cast(0 as float), (acc, x) -> acc + cast(x as float)) as match_prob
    from matching_tokens
    group by row_id_l, row_id_r
    """
    match_probabilities = spark.sql(sql)
    return match_probabilities


def compare_matching_records(original_df, matching_records, spark):

    matching_records.registerTempTable('matching_records')
    original_df.registerTempTable('original_df')

    sql = f"""
    select {sql_gen_col_selection_compare_cols(original_df)},
           mr.match_prob, cast(l.group == r.group as int) as real_group from

    matching_records as mr
    left join original_df as l
    on l.row_id = mr.row_id_l

    left join original_df as r
    on r.row_id = mr.row_id_r

    order by match_prob desc

    """
    return spark.sql(sql)


def get_feature_vectors(original_df, matching_records, spark):


    matching_records.registerTempTable('matching_records')
    original_df.registerTempTable('original_df')

    sql = f"""
    select
        mr.match_prob,
        levenshtein(l.concat, r.concat) as edit_distance,
        cast(l.group == r.group as int) as real_group,
        l.row_id as row_id_l, r.row_id as row_id_r
    from

    matching_records as mr
    left join original_df as l
    on l.row_id = mr.row_id_l

    left join original_df as r
    on r.row_id = mr.row_id_r

    order by match_prob desc

    """
    df = spark.sql(sql)

    assembler = VectorAssembler(inputCols=["match_prob", "edit_distance"], outputCol="features")
    return assembler.transform(df)

