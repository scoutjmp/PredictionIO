import pandas

def BatchProcess(users, item_attributes_map, user_items_map, verbose=0):
  """Construct user model.

  Args:
    users: a list of uid (UNUSED)
    item_attributes_map: iid -> [attribute]
    user_items_map: uid -> [iid], assume all actions are equal weight

  Returns:
    A pandas DataFrame. Column is user id, Index is attribute.
  """
  
  attribute_list = list(set(
    [a for attr_list in item_attributes_map.itervalues() for a in attr_list]))
  attribute_list.sort()

  if verbose:
    print 'Attributes', attribute_list

  item_list = item_attributes_map.keys()
  item_list.sort()

  item_df = pandas.DataFrame(index=attribute_list, columns=item_list).fillna(0)

  for item in item_list:
    item_attributes = item_attributes_map[item]
    item_series = item_df[item]
    for attribute in item_attributes:
      item_series[attribute] = 1

  if verbose:
    print 'item count:', len(item_list)

  user_df = pandas.DataFrame(
      index=attribute_list, columns=user_items_map.keys()).fillna(0)

  for user in user_df.columns:
    for item in user_items_map[user]:
      # skip unseen item
      if item not in item_list:
        continue
      user_df[user] = user_df[user] + item_df[item]

  if verbose:
    print 'user count:', len(user_df)

  # Normalization
  user_df = user_df.div(user_df.sum())

  if verbose:
    for user in user_df:
      print user, user_df[user]

  return user_df
    

def RecommendUserList(item, item_attributes, user_df):
  """
  Args:
    item: iid
    item_attributes: [attribute]
    user_df: A pandas DataFrame. uid x attribute. Output of BatchProcess.

  Returns;
    A pandas Series of sorted user. Index is user, value is score.
  """
  if item_attributes:
    item_series = pandas.Series(0., index=user_df.index)
    for attribute in item_attributes:
      if attribute in user_df.index:
        item_series[attribute] = 1.
  else:
    # if item has no attributes, assign equal weight
    item_series = pandas.Series(1., index=user_df.index)
  item_series /= item_series.sum()

  # dot product with user preference model
  user_item_df = user_df.mul(item_series, axis=0)
  user_item_score = user_item_df.sum()

  user_item_score.sort(ascending=False)

  return user_item_score


