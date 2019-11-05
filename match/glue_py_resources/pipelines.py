from sql_steps import *
from rules import get_test_data_rules


def get_features_df(df, spark):
    cols = ["first_name", "surname", "dob", "city", "email"]


    df = sql_concat_cols(df, spark, cols)

    df = tokenise_concat_field(df, spark)

    prob_lookup = get_token_probabilities_lookup(df, spark)

    rules = get_test_data_rules()

    potential_matches = get_potential_matches(df, spark, rules)

    word_combinations = get_word_combinations(potential_matches, spark)
    matching_tokens = find_exact_and_fuzzy_matching_tokens(word_combinations, spark)
    matching_tokens = matching_tokens_with_prob(matching_tokens, prob_lookup, spark)
    matching_records = reduce_probability_tokens_to_records(matching_tokens, spark)
    features = get_feature_vectors(df, matching_records, spark)
    return features


