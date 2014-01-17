import predictionio

APP_KEY = "z7P8GFbaDqrwpuuDCkDXqMr43uT4QVHv97wGYiJ3jFbD2S9uUsNmpjZgEsNgoOPy"
API_URL = "http://localhost:8000"

def import_testdata():
    client = predictionio.Client(APP_KEY, 1, API_URL)

    client.create_item("i5", ("t1",), {"custom1": "i0c1", "pio_startT" : 123456789010, 'description': "This movie is acted by Leonardo DiCaprio." })
    client.create_item("i6", ("t1","t2"), {"custom1": "i1c1", "custom2": "i1c2", "pio_startT" : 123456789011, 'description': "This movie is acted by Jim Carrey." })
    client.create_item("i7", ("t1","t2"), {"custom2": "i2c2", "pio_startT" : 123456789012 })
    client.create_item("i8", ("t1",), { "pio_startT" : 123456789013 })

    client.close()

if __name__ == '__main__':
    import_testdata()

