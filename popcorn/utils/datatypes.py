def in_array(sub_array, main_array):
    '''
    check weather sub_array is a sub array of main_array
    '''
    return all(map(lambda x: x in main_array, sub_array))
