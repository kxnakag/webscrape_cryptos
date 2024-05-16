# Web Scraping various crypto ranking sites
# https://coinranking.com/coins/brc-20
# By Ken Nakagawa
# 04/17/2024
# Capturing the following fields:
#  listing token name, website address, block explorer address, telegram address, twitter handle.
# Extract hyperlinks for each if possible.
# Thursday 05/02/2024
# Export to Json file
# Export to CSV

import time
import httpx
from selectolax.parser import HTMLParser
from urllib.parse import urljoin
from dataclasses import dataclass, asdict, fields
import json
import csv


@dataclass
class Item:
    coin_name: str
    price: float
    rank: str
    market_cap: float
    website: str


def get_html(url, **kwargs):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    if kwargs.get("page"):
        resp = httpx.get(
            url + str(kwargs.get("page")), headers=headers, follow_redirects=True
        )
    else:
        resp = httpx.get(url, headers=headers, follow_redirects=True)
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        print(
            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}. Page Limit Exceeded"
        )
        return False
    html = HTMLParser(resp.text)
    return html


def extract_text(html, sel):
    try:
        # Changed the above to add clean_data def
        text = html.css_first(sel).text(strip=True)
        # Below is like a mini pipeline
        return clean_data(text)
    except AttributeError:
        return None


def parse_search_page(html: HTMLParser):
    # This will print data of the first row
    coins = html.css("tr.table__row")
    # Coins are printing coins : [<Node tr>, <Node tr>, <Node tr>, <Node tr>
    for coin in coins:
        try:
            link = urljoin(
                "https://coinranking.com",
                coin.css_first("span.profile__name > a").attributes["href"],
            )
        except AttributeError:
            link = "None"
        # Let's skip over is link == "None"
        if link == "None":
            pass
        else:
            yield urljoin("https://coinranking.com", link)


def parse_item_page(html):
    new_item = Item(
        coin_name=extract_text(html, "h1 > a"),
        price=extract_text(html, " div.hero-coin__price > abbr"),
        rank=extract_text(html, "tr:nth-child(3) > td.stats__value"),
        # market_cap = extract_text(html, "td.stats__value > abbr"),
        market_cap=extract_text(html, "tr:nth-child(5) > td.stats__value > abbr"),
        website=extract_text(html, "tr:nth-child(1) > td > a"),
    )
    # Need dict format for Json
    return asdict(new_item)


def export_to_json(coins):
    with open("coins.json", "w", encoding="utf-8") as f:
        json.dump(coins, f, ensure_ascii=False, indent=4)
    print("Saved to Json")


def export_to_csv(coins):
    field_names = [field.name for field in fields(Item)]
    # Had to add utf-8 because I had a utf-8 character in my data
    with open("coins.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, field_names)
        writer.writeheader()
        writer.writerows(coins)

    print("Saved to csv")


def append_to_csv(coins):
    field_names = [field.name for field in fields(Item)]
    with open("appendcoinscsv.csv", "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, field_names)
        writer.writerows(coins)


# This gets put into extract_text()
def clean_data(value):
    chars_to_remove = [" million"]
    for char in chars_to_remove:
        if char in value:
            value = value.replace(char, "")
    # strip() removes any white space
    return value.strip()


def main():
    # Put this back for coins.append to work
    coins = []
    # Adding pagination
    baseurl = "https://coinranking.com/coins/brc-20?page="
    # Site goes up to page 16, if put in range over 16, process finishes at 17
    for x in range(1, 2):
        print(f"Gathering page: ", x)
        html = get_html(baseurl, page=x)

        # To get out of over page
        if html is False:
            break
        # Changing the names to fit looping over the urls
        product_urls = parse_search_page(html)
        for url in product_urls:
            print(url)
            html = get_html(url)
            # This was commented out for the append_to_csv
            # But I put it back in
            coins.append(parse_item_page(html))
            # Below is not working
            # append_to_csv(parse_item_page(html))
            time.sleep(0.1)

    export_to_json(coins)
    export_to_csv(coins)


if __name__ == "__main__":
    main()
