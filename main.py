from scraper import scrape_twitter
from aggregate import aggregate_files
from analyse import add_proxy_metrics
from store import store
from dotenv import load_dotenv


load_dotenv()


def main():
    scrape_twitter()
    aggregate_files()
    add_proxy_metrics()
    store()


if __name__ == '__main__':
    main()
