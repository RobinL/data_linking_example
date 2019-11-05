from graphframes import GraphFrame

#  Need a dataset of edges and nodes


def get_graph(orig_df, predictions, orig_df_id_col="row_id", predictions_id_col="id"):
    predictions_nodes = orig_df.withColumnRenamed(orig_df_id_col, "id")
    predictions_edges = predictions.withColumnRenamed(f"{predictions_id_col}_l", "src").withColumnRenamed(
        f"{predictions_id_col}_r", "dst").filter(predictions.prediction == 1.0)
    return GraphFrame(predictions_nodes, predictions_edges)


def get_connected_components(*arg, **kwarg):
    g = get_graph(*arg, **kwarg)
    return g.connectedComponents()


