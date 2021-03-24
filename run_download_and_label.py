import copy
from image_downloader import main

if __name__ == '__main__':

    # FOLDER_NAME_INDEX = 9
    # NAME_INDEX = 12
    # BIRTHDATE_INDEX = 14
    IMAGE_NUM = 50

    # ARGV = ['-e', 'Google', '-d', 'chrome_headless', '-n', f'{IMAGE_NUM}', '-j', '10', '-o',
    #         'img/google/kids10/{folder_name}', '-F', '-S', '{name}', '-B', "{birthdate}"]

    with open('../query_results/IMDB/query_2010_2011_47.csv', 'r', encoding="utf-8") as file:
        line_list = file.read().splitlines()

    search_pairs = [(line.split(",")[0], line.split(",")[2]) for line in line_list]

    for search_pair in search_pairs:
        # argv = copy.deepcopy(ARGV)
        # argv[FOLDER_NAME_INDEX] = ARGV[FOLDER_NAME_INDEX].format(folder_name=search_pair[0].replace(" ", "_"))
        # argv[NAME_INDEX] = ARGV[NAME_INDEX].format(name=search_pair[0])
        # argv[BIRTHDATE_INDEX] = ARGV[BIRTHDATE_INDEX].format(birthdate=search_pair[1])
        folder_name = search_pair[0].replace(" ", "_")
        name = search_pair[0]
        birthdate = search_pair[1]
        argv = ['-e', 'Google', '-d', 'chrome_headless', '-n', f'{IMAGE_NUM}', '-j', '10', '-o',
                f'img/google/kids_actor_10/{folder_name}', '-F', '-S', f'{name}', '-B', f"{birthdate}"]

        main(argv=argv)
