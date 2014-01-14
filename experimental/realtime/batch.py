import pandas

def BatchProcess(users, item_attributes_map, user_actions_map, verbose=0):
  """Construct user model.

  Args:
    users: a list of uid
    item_attributes_map: iid -> [attribute]
    user_actions_map: uid -> [iid], assume all actions are equal weight

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
      index=attribute_list, columns=user_actions_map.keys()).fillna(0)

  for user in user_df.columns:
    for item in user_actions_map[user]:
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
    





