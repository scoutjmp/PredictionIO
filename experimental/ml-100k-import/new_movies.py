import predictionio

from app_config import APP_KEY, API_URL, THREADS, REQUEST_QSIZE

if __name__ == '__main__':
    client = predictionio.Client(APP_KEY, THREADS, API_URL, qsize=REQUEST_QSIZE)

    client.acreate_item("Star Trek Into Darkness", ("movie",), {"Action": 1, "Adventure": 1, "Sci-Fi": 1})
    client.acreate_item("Gravity", ("movie",), {"Drama": 1, "Sci-Fi": 1, "Thriller": 1})
    client.acreate_item("Les Miserables", ("movie",), {"Drama": 1, "Musical": 1, "Romance": 1})
    client.acreate_item("Iron Man 3", ("movie",), {"Action": 1, "Adventure": 1, "Fantasy": 1, "Sci-Fi": 1})
    client.acreate_item("Before Sunset", ("movie",), {"Drama": 1, "Romance": 1})
    client.acreate_item("Monsters University", ("movie",), {"Animation": 1, "Adventure": 1, "Comedy": 1, "Family": 1, "Fantasy": 1})
    client.acreate_item("Frozen", ("movie",), {"Animation": 1, "Adventure": 1, "Comedy": 1, "Family": 1, "Fantasy": 1, "Musical": 1})
    client.acreate_item("Jiro Dreams of Sushi", ("movie",), {"Documentary": 1})
    client.acreate_item("Black Swan", ("movie",), {"Drama": 1, "Mystery": 1, "Thriller": 1})
    client.acreate_item("Inception", ("movie",), {"Action": 1, "Adventure": 1, "Mystery": 1, "Sci-Fi": 1, "Thriller": 1})

    client.close()
