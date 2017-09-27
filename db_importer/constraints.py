def create_index(graph, node_label, props, current_indexes = None):
    if current_indexes is None or type(current_indexes) is not list:
        current_indexes = graph.schema.get_indexes(node_label)

    if type(props) is str:
        if props not in current_indexes:
            graph.schema.create_index(node_label, props)
    elif type(props) is list:
        for prop in props:
            if prop not in current_indexes:
                graph.schema.create_index(node_label, prop)

def create_unique_constraint(graph, node_label, props, current_constraints = None):
    if current_constraints is None or type(current_constraints) is not list:
        current_constraints = graph.schema.get_uniqueness_constraints(node_label)

    if type(props) is str:
        if props not in current_constraints:
            graph.schema.create_uniqueness_constraint(node_label, props)
    elif type(props) is list:
        for prop in props:
            if prop not in current_constraints:
                graph.schema.create_uniqueness_constraint(node_label, prop)

def define_data_constraints(graph):
    # TIME FRAME constraints
    create_unique_constraint(graph, 'TIME_FRAME', 'timestamp')
    tf_indexes = graph.schema.get_indexes('TIME_FRAME')
    create_index(graph, 'TIME_FRAME', ['hour', 'month', 'year', 'part_of_day', 'day_in_week'], tf_indexes)

    # USER constraints
    create_unique_constraint(graph, 'USER', 'oid')
    create_index(graph, 'USER', 'money_spent')

    # PRODUCT constraints
    create_unique_constraint(graph, 'PRODUCT', 'oid')
    create_index(graph, 'PRODUCT', 'price')

    # ORDER constraints
    create_unique_constraint(graph, 'ORDER', 'oid')

    # CAT constraints
    create_unique_constraint(graph, 'CAT', 'oid')

    # TAG constraints
    create_unique_constraint(graph, 'TAG', 'oid')

    print 'Data constraints defined'
