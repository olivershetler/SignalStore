

def dataarray_isequal(dataarray1, dataarray2):
    assertions = {}
    assertions["name"] = dataarray1.name == dataarray2.name
    assertions["attrs"] = {}
    for attr in dataarray1.attrs:
        assertions["attrs"][attr] = dataarray1.attrs[attr] == dataarray2.attrs[attr]
    # check for attributes in dataarray2 that are not in dataarray1
    for attr in dataarray2.attrs:
        if attr not in dataarray1.attrs:
            assertions["attrs"][attr] = False
    assertions["dims"] = set(dataarray1.dims) == set(dataarray2.dims)
    if assertions["dims"] == False:
        # elements in the intersection of dims are true
        assertions["dims"] = {}
        for dim in set(dataarray1.dims) & set(dataarray2.dims):
            assertions["dims"][dim] = True
        # elements in the difference of dims are false
        for dim in set(dataarray1.dims) - set(dataarray2.dims):
            assertions["dims"][dim] = False
        for dim in set(dataarray2.dims) - set(dataarray1.dims):
            assertions["dims"][dim] = False
    # get the union of the coordinates
    coords = list(set(dataarray1.coords) | set(dataarray2.coords))
    assertions["coords"] = {}
    for coord in coords:
        if coord not in dataarray1.coords or coord not in dataarray2.coords:
            assertions["coords"][coord] = False
            continue
        if dataarray1.coords[coord].shape != dataarray2.coords[coord].shape:
            assertions["coords"][coord] = False
        else:
            assertions["coords"][coord] = all(dataarray1.coords[coord] == dataarray2.coords[coord])

    if dataarray1.data.shape != dataarray2.data.shape:
        assertions["data"]  = False
    else:
        assertions["data"] = (dataarray1.data == dataarray2.data).all()
    agg_assertions = {}
    for key in assertions:
        if isinstance(assertions[key], dict):
            agg_assertions[key] = all(assertions[key].values())
        else:
            agg_assertions[key] = assertions[key]
    result =  all(agg_assertions.values())
    if result == False:
        print(assertions)
    return result
