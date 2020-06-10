import pandas as pd


def strip(x):
    if x:
        return x.strip()
    return x


def add_date(x):
    if not x:
        return x
    if x.count('/') == 1:
        return '01/'+x
    if x.count('/') == 0:
        return '01/01/'+x
    return x



# Book
n = 100  # Number of book to index    

b_df = pd.read_csv('crawldata/book.csv')
b_df = b_df.dropna()
b_df = b_df.head(n)
b_df = b_df.astype('str')
b_df['datePublished'] = b_df['datePublished'].apply(add_date)
b_df['images'] = b_df['images'].apply(lambda x: x.split(":")[3]).apply(lambda x: x.split(",")[0]).apply(lambda x: x.split("'")[1])


b_df.drop(columns=['image_urls'], axis=1, inplace=True)

b_df['images'] = b_df['images'].apply(strip)
b_df['category'] = b_df['category'].apply(strip)
b_df['sub_category'] = b_df['sub_category'].apply(strip)
b_df['title'] = b_df['title'].apply(strip)
b_df['author'] = b_df['author'].apply(strip)
b_df['description'] = b_df['description'].apply(strip)

b_df.drop_duplicates(subset=['title'], inplace=True)

b_df.to_csv('processeddata/book.csv', index=False)




# Author
a_df = b_df[['author']].copy()
a_df.rename(columns={'author': 'name'}, inplace=True)
a_df.drop_duplicates('name', 'first', inplace=True)

a_df.to_csv('processeddata/author.csv', index=False)



# Category
c_df = pd.read_csv('crawldata/category.csv')
c_df.drop('images', axis=1, inplace=True)
c_df = c_df.dropna()
c_df = c_df.astype('str')


# Sub_Category
data = []
for index, row in c_df.iterrows():
    for sub in row[1].split(','):
        data.append([row[0], sub])

sub_df = pd.DataFrame(data, columns=['category', 'name'])
sub_df['name'] = sub_df['name'].apply(strip)
sub_df.to_csv('processeddata/sub_category.csv', index=False)


c_df = c_df['category'].to_frame()
c_df.rename(columns={'category': 'name'}, inplace=True)
c_df['name'] = c_df['name'].apply(strip)

c_df.to_csv('processeddata/category.csv', index=False)