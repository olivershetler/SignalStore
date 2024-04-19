






# 1D data (e.g. spike labels or spike times) have to be saved as 2D with an extra dimension e.g. (index, 1)
# This is because of the xarray function "is_list_of_strings" that requires the extra dimension
# 1D data will be encoded as 2D with the extra dimension termed "1"
