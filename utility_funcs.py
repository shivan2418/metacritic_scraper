import os


def make_dir_if_not_exist(path):

    try:
        os.makedirs(path)

    except FileExistsError:
        pass

    except Exception as e:
        print(e)



if __name__ == '__main__':
    path = 'Jedi/rose/'

    make_dir_if_not_exist(path)
