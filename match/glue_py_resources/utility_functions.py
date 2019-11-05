
def sql_gen_concat_cols(cols, delimiter = " "):
    """
    Generate a sql expression to concatenate multiple columns
    together using a delimiter

    e.g. ["a", "b", "c"]
    => concat("a", " ", "b", " ", "c")
    """

    surrounded = [f'coalesce({name}, "")' for name in cols]
    with_spaces = f', "{delimiter}", '.join(surrounded)

    return f'concat({with_spaces})'

def sql_gen_single_blocking_rule(rule):
    """
    Generate sql for a single blocking rule
    eg of a rule:
        'l.first_name = r.first_name and l.surname = r.surname
    """

    return f"""
    select
        l.tokens as tokens_l,
        l.row_id as row_id_l,
        r.tokens as tokens_r,
        r.row_id as row_id_r,
        "rule" as rule
    from df as l
    left join df as r
    on
    {rule}
    where l.row_id < r.row_id
    """


def sql_gen_col_selection_compare_cols(df):
    """
    Compare cols in df
    Example output from a input df with columns [first_name,  surname]
    l.first_name as first_name_l, r.first_name as first_name_r, l.surname as surname_l, r.surname as surname_r
    """

    l = [f"l.{c} as {c}_l" for c in df.columns]
    r = [f"r.{c} as {c}_r" for c in df.columns]
    both = zip(l, r)
    flat_list = [item for sublist in both for item in sublist]
    return ", ".join(flat_list)


def get_metrics(df, spark, label_col="real_group", prediction_col="prediction", row_id_l_col="row_id_l", row_id_r_col="row_id_r"):

    # THIS IS CURRENTLY INCORRECT BECAUSE IT DOESN'T MERGE THE
    # PREDICTIONS WITH THE ORIGINAL DATASET
    # SO IT MISSES FALSE NEGATIVES I.E. cases which really are a match but are not predicted as such

    # It's easier to do this once you have the connected components, see notebook 13.

    df.registerTempTable("df")
    sql = f"""
    select
      distinct {row_id_l_col},
      {row_id_r_col},
      CASE
          WHEN {label_col} == 1 and {prediction_col} == 1 THEN 'tp'
          WHEN not {label_col} == 1 and {prediction_col} == 1 THEN 'fp'
          WHEN {label_col} == 1 and not {prediction_col} == 1 THEN 'fn'
      END as accuracy_class
      from df

    """
    comparisons = spark.sql(sql)

    comparisons.registerTempTable("comparisons")
    sql = """
    select accuracy_class, count(*) as count
    from comparisons
    group by accuracy_class
    """
    accurancy_class = spark.sql(sql)
    ac = accurancy_class.collect()

    results_dict = {}
    for i in ac:
        results_dict[i["accuracy_class"]] = i["count"]

    for k in ['tp', 'fp', 'fn']:
        if k not in results_dict:
            results_dict[k] = 0

    n = df.count()
    num_comparisons = n * (n - 1) / 2

    tp = results_dict['tp']
    fp = results_dict['fp']
    fn = results_dict['fn']
    tn = num_comparisons - tp - fp - fn

    accuracy_dict = {
        "true_positive_count": tp,
        "true_negative_count": tn,
        "false_positive_count": fp,
        "false_negative_count": fn,
        "sensitivity": tp/(tp+fn),
        "precision": tp/(tp+fp)
    }
    return accuracy_dict
