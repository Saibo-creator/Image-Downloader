# -*- coding: utf-8 -*-
# author: Yabin Zheng
# Email: sczhengyabin@hotmail.com

from __future__ import print_function

import argparse
from ImageLabelingPackage.ExifImageAgeLabeler import ExifImageAgeLabeler
import crawler
import downloader
import glob
import os
import sys
import logging
from time import sleep
from selenium.common.exceptions import WebDriverException



def google_download(argv):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description="Image Downloader")
    parser.add_argument("keywords", type=str,
                        help='Keywords to search. ("in quotes")')
    parser.add_argument("--engine", "-e", type=str, default="Google",
                        help="Image search engine.", choices=["Google", "Bing", "Baidu"])
    parser.add_argument("--driver", "-d", type=str, default="chrome_headless",
                        help="Image search engine.", choices=["chrome_headless", "chrome", "phantomjs"])
    parser.add_argument("--max-number", "-n", type=int, default=100,
                        help="Max number of images download for the keywords.")
    parser.add_argument("--num-threads", "-j", type=int, default=50,
                        help="Number of threads to concurrently download images.")
    parser.add_argument("--timeout", "-t", type=int, default=20,
                        help="Seconds to timeout when download an image.")
    parser.add_argument("--output", "-o", type=str, default="./download_images",
                        help="Output directory to save downloaded images.")
    parser.add_argument("--safe-mode", "-S", action="store_true", default=False,
                        help="Turn on safe search mode. (Only effective in Google)")
    parser.add_argument("--label-age", "-l", action="store_true", default=True,
                        help="extract the age ")
    parser.add_argument("--birthdate", "-B", type=str, default=None,
                        help="birthdate of the searched person")
    parser.add_argument("--face-only", "-F", action="store_true", default=False,
                        help="Only search for ")
    parser.add_argument("--proxy_http", "-ph", type=str, default=None,
                        help="Set http proxy (e.g. 192.168.0.2:8080)")
    parser.add_argument("--proxy_socks5", "-ps", type=str, default=None,
                        help="Set socks5 proxy (e.g. 192.168.0.2:1080)")

    args = parser.parse_args(args=argv)
    # argv = ['-e', 'Google', '-d', 'chrome_headless', '-n', '40', '-j', '10', '-o', 'img/google/kids10/Colin_Baiocchi', '-F', '-S', 'Colin Baiocchi']

    proxy_type = None
    proxy = None
    if args.proxy_http is not None:
        proxy_type = "http"
        proxy = args.proxy_http
    elif args.proxy_socks5 is not None:
        proxy_type = "socks5"
        proxy = args.proxy_socks5

    if args.label_age and args.birthdate is None:
        raise RuntimeError("Birthdate is necessary if args.label_age is True")

    sleep_time = 2
    num_retires = 4

    for x in range(num_retires):
        """
        selenium.common.exceptions.WebDriverException: Message: unknown error: Chrome failed to start: exited abnormally.
        (unknown error: DevToolsActivePort file doesn't exist)
        (The process started from chrome location /usr/bin/chromium is no longer running, so ChromeDriver is assuming that Chrome has crashed.)

        """
        try:
            crawled_urls = crawler.crawl_image_urls(args.keywords,
                                                    engine=args.engine, max_number=args.max_number,
                                                    face_only=args.face_only, safe_mode=args.safe_mode,
                                                    proxy_type=proxy_type, proxy=proxy,
                                                    browser=args.driver)
            downloader.download_images(image_urls=crawled_urls, dst_dir=args.output,
                                       concurrency=args.num_threads, timeout=args.timeout,
                                       proxy_type=proxy_type, proxy=proxy,
                                       file_prefix=args.keywords)
        except WebDriverException:
            sleep(sleep_time)
            pass
        else:
            break

    ageLabeler = ExifImageAgeLabeler()
    # dir = "Image-Downloader/download_images/google/kids10"
    files = os.listdir(args.output)
    # files = [file for file in files if os.path.isfile(file)]
    for fn in files:
        age, _ = ageLabeler.label_age(fn, birthdate_str=args.birthdate, image_dir=args.output)
        if age is not None:
            src = os.path.join(args.output, fn)
            imagename_with_age = os.path.splitext(fn)[0] + "|{}".format(age) + os.path.splitext(fn)[1]
            dst = os.path.join(args.output, imagename_with_age)
            os.rename(src, dst)
        else:
            src = os.path.join(args.output, fn)
            os.remove(src)

    logger.info("Finished.")

    return len(os.listdir(args.output))


if __name__ == '__main__':
    # main(sys.argv[1:])
    #
    argv = ['-e', 'Google', '-d', 'chrome_headless', '-n', '100', '-j', '10', '-o',
            'img/google/kids10/Colin_Baiocchi', '-F', '-S', 'Colin Baiocchi', '-B', "2005-01-01"]
    google_download(argv=argv)