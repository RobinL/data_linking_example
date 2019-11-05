from utility_functions import sql_gen_col_selection_compare_cols

def get_cartesian_pairs(predicted_groups, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    predicted_groups.registerTempTable("pg")

    sql = f"""
    select
        pg_l.{real_group_col} as {real_group_col}_l,
        pg_r.{real_group_col} as {real_group_col}_r,
        pg_l.{predicted_group_col} as {predicted_group_col}_l,
        pg_r.{predicted_group_col} as {predicted_group_col}_r,
        pg_l.{id_col} as {id_col}_l,
        pg_r.{id_col} as {id_col}_r,
        pg_l.{real_group_col} = pg_r.{real_group_col} as is_part_of_actual_group,
        pg_l.{predicted_group_col} = pg_r.{predicted_group_col} as is_estimated_as_same_group
    from pg as pg_l
    cross join pg as pg_r
    where pg_l.{id_col} < pg_r.{id_col}
    """

    return spark.sql(sql)


def get_real_pairs(predicted_groups, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    predicted_groups.registerTempTable("pg")

    sql = f"""
    select
        pg_l.{real_group_col} as {real_group_col}_l,
        pg_r.{real_group_col} as {real_group_col}_r,
        pg_l.{predicted_group_col} as {predicted_group_col}_l,
        pg_r.{predicted_group_col} as {predicted_group_col}_r,
        pg_l.{id_col} as {id_col}_l,
        pg_r.{id_col} as {id_col}_r,
        true as is_part_of_actual_group,
        pg_l.{predicted_group_col} = pg_r.{predicted_group_col} as is_estimated_as_same_group
    from pg as pg_l
    left join pg as pg_r
    on pg_l.{real_group_col} = pg_r.{real_group_col}
    where pg_l.{id_col} < pg_r.{id_col}
    """

    return spark.sql(sql)


def get_predicted_pairs(predicted_groups, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    predicted_groups.registerTempTable("pg")

    sql = f"""
    select
        pg_l.{real_group_col} as {real_group_col}_l,
        pg_r.{real_group_col} as {real_group_col}_r,
        pg_l.{predicted_group_col} as {predicted_group_col}_l,
        pg_r.{predicted_group_col} as {predicted_group_col}_r,
        pg_l.{id_col} as {id_col}_l,
        pg_r.{id_col} as {id_col}_r,
        pg_l.{real_group_col} = pg_r.{real_group_col} as is_part_of_actual_group,
        true as is_estimated_as_same_group

    from pg as pg_l
    left join pg as pg_r
    on pg_l.{predicted_group_col} = pg_r.{predicted_group_col}
    where pg_l.{id_col} < pg_r.{id_col}

    """

    return spark.sql(sql)


def categorise_pairs(all_pairs, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):
    all_pairs.registerTempTable("pairs")
    sql = f"""
    select
            {real_group_col}_l,
            {real_group_col}_r,
            {predicted_group_col}_l,
            {predicted_group_col}_r,
            {id_col}_l,
            {id_col}_r,
            is_part_of_actual_group,
            is_estimated_as_same_group,

            CASE
            WHEN is_part_of_actual_group and is_estimated_as_same_group THEN 'tp'
            WHEN not is_part_of_actual_group and is_estimated_as_same_group THEN 'fp'
            WHEN is_part_of_actual_group and not is_estimated_as_same_group THEN 'fn'
            WHEN not is_part_of_actual_group and not is_estimated_as_same_group THEN 'tn'
            END as accuracy_class

            from pairs
    """
    return spark.sql(sql)


def get_real_pred_pairs(predicted_groups, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):
    rp = get_real_pairs(predicted_groups, spark, id_col=id_col,
                        real_group_col=real_group_col, predicted_group_col=predicted_group_col)
    pp = get_predicted_pairs(predicted_groups, spark, id_col=id_col,
                             real_group_col=real_group_col, predicted_group_col=predicted_group_col)
    all_pairs = rp.union(pp).drop_duplicates(
        subset=[f"{id_col}_l", f"{id_col}_r"])
    categorised_pairs = categorise_pairs(
        all_pairs, spark, id_col=id_col, real_group_col=real_group_col, predicted_group_col=predicted_group_col)
    return categorised_pairs


def get_accuracy_statistics(all_real_and_predicted_pairs, orig_data, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    all_real_and_predicted_pairs.registerTempTable("pairs")

    sql = """
    select accuracy_class, count(*) as count
    from pairs
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

    n = orig_data.count()
    num_comparisons = int(n * (n - 1) / 2)

    tp = results_dict['tp']
    fp = results_dict['fp']
    fn = results_dict['fn']
    tn = num_comparisons - tp - fp - fn

    accuracy_dict = {
        "total_pairs": tp + tn + fp+fn,
        "true_positive_count": tp,
        "true_negative_count": tn,
        "false_positive_count": fp,
        "false_negative_count": fn,
        "sensitivity": tp/(tp+fn),
        "precision": tp/(tp+fp)
    }
    return accuracy_dict


def get_accuracy_statistics_cartesian(all_pairs, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    all_pairs.registerTempTable("all_pairs")

    sql = """
    select accuracy_class, count(*) as count
    from all_pairs
    group by accuracy_class
    """
    accurancy_class = spark.sql(sql)
    ac = accurancy_class.collect()

    results_dict = {}
    for i in ac:
        results_dict[i["accuracy_class"]] = i["count"]

    for k in ['tp', 'fp', 'fn', 'tn']:
        if k not in results_dict:
            results_dict[k] = 0

    tp = results_dict['tp']
    fp = results_dict['fp']
    fn = results_dict['fn']
    tn = results_dict['tn']

    accuracy_dict = {
        "total_pairs": tp + tn + fp+fn,
        "true_positive_count": tp,
        "true_negative_count": tn,
        "false_positive_count": fp,
        "false_negative_count": fn,
        "sensitivity": tp/(tp+fn),
        "precision": tp/(tp+fp)
    }
    return accuracy_dict


def get_record_comparisons_labeled_with_accuracy_category(real_pred_pairs, orig_data, predictions, spark, id_col="row_id", real_group_col="group", predicted_group_col="component"):

    # Pairs comes from real_pred pairs
    real_pred_pairs.registerTempTable('pairs')
    orig_data.registerTempTable('orig_data') # Original data frame
    predictions.registerTempTable('predictions') # Output from logit model

    sql = f"""
    select {sql_gen_col_selection_compare_cols(orig_data)}, accuracy_class, match_prob, edit_distance, component_l, component_r
    from pairs as p

    inner join orig_data as l
    on l.{id_col} = p.id_l
    inner join orig_data as r
    on r.{id_col} = p.id_r

    inner join predictions as pred
    on r.{id_col} = pred.{id_col}_r
    and l.{id_col} = pred.{id_col}_l

    """

    return spark.sql(sql)
