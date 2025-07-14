class PCYItemsGenerator:
  def __init__(self, dataframe, item, basket, support):
    self.df = dataframe
    self.item = item
    self.basket = basket
    self.support = support
    logging.info(f"Frequent items generator initialized with support threshold of {support*100}%")

  def create_transactions(self):
    logging.info(f"Frequent items generator creating transactions")
    self.hash_table = pd.DataFrame(books_rating_df['Title'].unique()).reset_index()
    self.hash_table.columns = ['item_no', 'Title']
    self.df = pd.merge(self.df, self.hash_table, on='Title', how='left')
    self.transactions = self.df.groupby(self.basket)['item_no'].apply(list).tolist()
    self.transactions = [list(set(transaction)) for transaction in self.transactions]
    logging.info(f"Transactions created")
    return self.transactions

  def first_pass_apriori(self):
    logging.info(f"Beginning first pass apriori")
    all_items = [item_no for sublist in self.transactions for item_no in sublist]
    self.item_counts = pd.Series(all_items).value_counts()
    self.item_counts = self.item_counts.reset_index()
    self.item_counts.columns = ['item_no', 'count']
    logging.info(f"First pass apriori completed")
    return self.item_counts

  def between_passes(self):
    self.support_threshold = len(self.transactions)*self.support
    self.frequent_singletons = self.item_counts[self.item_counts['count'] >= self.support_threshold]
    #we don't need new indices for frequent singletons because the indices were created based on sorting the counts in the first place
    logging.info(f"Support threshold is at {self.support_threshold} items")
    logging.info(f"{len(self.frequent_singletons)} frequent singletons identified!")
    self.num_buckets = len(self.transactions) // 100
    buckets = [0] * self.num_buckets
    for transaction in self.transactions:
      for i in range(len(transaction)):
          for j in range(i + 1, len(transaction)):
              a, b = sorted([transaction[i], transaction[j]])
              bucket_idx = hash((a, b)) % self.num_buckets
              buckets[bucket_idx] += 1
    self.bitmap = [1 if count >= self.support_threshold else 0 for count in buckets]
    return self.frequent_singletons, self.support_threshold

  def second_pass_apriori(self):
    logging.info(f"Beginning second pass apriori")
    self.pairs_count = {}
    baskets_to_remove = []
    for idx, basket in enumerate(self.transactions):
      basket = [item_no for item_no in basket if item_no in list(self.frequent_singletons['item_no'])]
      if len(basket) <= 1:
        baskets_to_remove.append(idx)
        continue
      for i in range(len(basket)):
        for j in range(i+1, len(basket)):
          if self.bitmap[hash((basket[i], basket[j])) % self.num_buckets] == 1:
            if (basket[i], basket[j]) in self.pairs_count:
              self.pairs_count[(basket[i], basket[j])] += 1
            else:
              self.pairs_count[(basket[i], basket[j])] = 1
    for idx in sorted(baskets_to_remove, reverse=True):
      self.transactions.pop(idx)
    self.frequent_pairs = {pair: count for pair, count in self.pairs_count.items() if count > self.support_threshold}
    self.frequent_pairs = pd.DataFrame.from_dict(self.frequent_pairs, orient='index', columns=['count'])
    logging.info(f"Second pass apriori completed")
    logging.info(f"{len(self.frequent_pairs)} frequent pairs identified!")
    return self.frequent_pairs

  def __call__(self):
    self.create_transactions()
    self.first_pass_apriori()
    self.between_passes()
    self.second_pass_apriori()
    self.frequent_singletons = pd.merge(self.frequent_singletons, self.hash_table, on='item_no', how='left')
    self.frequent_singletons = self.frequent_singletons.drop(columns=['item_no'])
    self.frequent_pairs[['item_no_1', 'item_no_2']] = pd.DataFrame(self.frequent_pairs.reset_index()['index'].tolist(), index = self.frequent_pairs.index)
    self.frequent_pairs.reset_index(drop=True)
    self.frequent_pairs = pd.merge(self.frequent_pairs, self.hash_table, left_on='item_no_1', right_on='item_no', how='left')
    self.frequent_pairs = pd.merge(self.frequent_pairs, self.hash_table, left_on='item_no_2', right_on='item_no', how='left')
    self.frequent_pairs = self.frequent_pairs.drop(columns=['item_no_1', 'item_no_2', 'item_no_x', 'item_no_y'])
