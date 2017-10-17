import pandas as pd
import os


def preprocess_ios(ios_path):
    with open(ios_path) as f:
        app_info = (f.readlines())

    app_info = map(lambda app: app.translate(None, "\n'[]u ").replace(',', '.').split('\t'), app_info)

    non_games = ['Books', 'Bsiness', 'Catalogs', 'Finance', 'Food&Drink', 'Health&Fitness', 'Lifestyle',
                 'Magazines&Newspapers', 'Medical', 'Navigation', 'News', 'Photo&Video', 'Prodctivity', 'Reference',
                 'Shopping', 'SocialNetworking', 'Travel', 'Utilities', 'Weather']

    def parse_categories(app):
        app_categories = app[1].split('.')
        first = app_categories[0]
        if first == 'Games':
            if len(app_categories) == 1 or app_categories[1] in non_games:
                return [app[0], 'Games_Entertainment', app[1], app[2]]
            elif app_categories[1] == 'Edcation':
                return [app[0], '{}_{}'.format(first, 'Edcational'), app[1], app[2]]
            elif app_categories[1] == 'Dice':
                return [app[0], '{}_{}'.format(first, 'Casino'), app[1], app[2]]
            else:
                return [app[0], '{}_{}'.format(first, app_categories[1]), app[1], app[2]]
        elif first == 'Magazines&Newspapers':
            return [app[0], 'News', app[1], app[2]]
        else:
            return [app[0], first, app[1], app[2]]

    app_info = map(parse_categories, app_info)

    return app_info


def preprocess_android(android_path_):
    with open(android_path_) as f:
        app_info = (f.readlines())

    categories_mapping = pd.read_csv(path_ + 'categories_mapping', sep='\t')

    app_info = map(lambda app: app.translate(None, "\n $").split(','), app_info)
    app_info = map(lambda app: [app[0], categories_mapping.loc[categories_mapping['android'] == app[2],
                                                               'ios'].values[0], app[2], app[3]], app_info)

    return app_info


def save_to_csv(datain, outpath):
    df = pd.DataFrame(data=datain, columns=['bundle', 'category', 'original_categories', 'price'])
    df.loc[df.price == '', 'price'] = '0'
    df['price'] = df['price'].astype(float)
    df.to_csv(outpath, sep='\t', header=False, index=False)


def main():
    path_ = '/home/shlomi/Documents/shlomi/profiling/user-profile/scrapers/'

    ios = preprocess_ios(path_ + 'ios/ios_app_store_info')
    android = preprocess_android(path_ + 'android/total_android_apps_info')

    save_to_csv(ios + android, path_ + 'apps_info')




