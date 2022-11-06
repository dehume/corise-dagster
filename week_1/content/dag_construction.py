from dagster import graph, op


@op
def A():
    return "A"


@op
def B(start_after):
    return "B"


@op
def C(start_after):
    return "C"


@graph
def linear():
    a = A()
    b = B([a])
    C([b])


# @graph
# def fan_out():
#     a = A()
#     b_1 = B([a])
#     b_2 = B([a])
#     b_3 = B([a])
#     C([b_1, b_2, b_3])


# @graph
# def challenge():
#     a = A()
#     b_1 = B([a])
#     b_2 = B([a])
#     b_3 = B([a])
#     C([b_1, b_3])
#     C([a, b_2])
#     C([a])


# @graph
# def unconnected():
#     a = A()
#     a_1 = A()
#     b = B(a)
