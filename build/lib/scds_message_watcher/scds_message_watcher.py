import sys
import time
import logging
import psycopg2

from lxml import etree

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pprint import pprint

class ScdsMessageWatcher:
    def __init__(self, src_path):
        self.__src_path = src_path
        self.__event_handler =  FileSystemEventHandler()
        self.__event_observer = Observer()

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.sto()

    def start(self):
        self.__schedule()
        self.__event_observer.start()

    def stop(self):
        self.__event_observer.stop()
        self.__event_observer.join()

    def __schedule(self):
        self.__event_observer.schedule(
            self.__event_handler,
            self.__src_path,
            recursive=True
        )

        # Tengo que sobrecargar el método para poder escuchaar únicamente los creados
        self.__event_handler.on_created = self.__on_created

    def __on_created(self, event):
        self.read_file(event.src_path)

    def read_file(self, file_path):
        # Reading file
        parsed_file = etree.parse(file_path)
        # To string
        stringified_file = etree.tostring(parsed_file)


if __name__ == "__main__":
    src_path = sys.argv[1] if len(sys.argv) > 1 else '.'
    ScdsMessageWatcher(src_path).run()