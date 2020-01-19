def coommon_cleaning(string, loader_context):
    """
    :param string:依据xpath提取的value
     :param loader_context: load的内容， 包括item的key， response， Selector
    :return:修正后的值
    """
    if string:
        return string[0]
    return None



