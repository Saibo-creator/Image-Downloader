import copy
import shutil
import os
import time
import yaml
import argparse

import csv
import logging
from os.path import abspath,dirname
from imdb import IMDb
from imdb._exceptions import IMDbParserError
from ImageLabelingPackage.ImageDownloadLabeler import ImageDownloader
from deepface import DeepFace
from PyS3Upload.PyS3Uploader import S3Uploader
from PyS3Upload.helper import get_time_identifier
from image_downloader import google_download
from concurrent.futures import ProcessPoolExecutor, as_completed

def verify(reference_img_path, img_path):
    try:
        result = DeepFace.verify(reference_img_path, img_path, enforce_detection=False,
                                 model_name='VGG-Face')
    except AttributeError as e:  # case when input image is a pdf, then the package can not read it.
        log.info(e)
        result = {"verified": False}

    if not result["verified"]:
        os.remove(img_path)
        return 0
    else:
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", default="imdb")

    args = parser.parse_args()
    config_file = args.config

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    with open(dirname(abspath(__file__)) + f'/../configs/{config_file}.yaml') as f:

        config = yaml.load(f, Loader=yaml.FullLoader)

    IMAGE_RECALL_NUM = config["IMAGE_RECALL_NUM"]
    REMOVE_REFERENCE_IMG = config["REMOVE_REFERENCE_IMG"]
    REMOVE_LOCAL_AFTER_UPLOAD = config["REMOVE_LOCAL_AFTER_UPLOAD"]
    MULTIPROCESS = config["MULTIPROCESS"]
    NUM_WORKERS = config["NUM_WORKERS"]

    ia = IMDb()

    startTime = int(round(time.time()))

    line_list = []
    query_path = os.path.join(dirname(dirname(abspath(__file__))), config["query_path"])
    query_path_, ext = os.path.splitext(query_path)

    runtime_query_path = query_path_+"_runtime"+ext
    if not os.path.exists(runtime_query_path):
        shutil.copy(query_path, runtime_query_path)

    with open(runtime_query_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(spamreader):
            if i == 0:
                col_names = row
            else:
                line_list.append(row)

    imageDownloader = ImageDownloader()
    col_names_dict = {col_name: index for index, col_name in enumerate(col_names)}
    img_root_dir = config["img_root_dir"]
    while len(line_list) > 0:
        line = line_list.pop(0)
        name = line[col_names_dict["itemLabel"]]
        birthdate = line[col_names_dict["dateOfBirth"]]
        if "image" in col_names_dict:
            image_url = line[col_names_dict["image"]]
        elif "IMDb_ID" in col_names_dict:
            imdb_id = line[col_names_dict["IMDb_ID"]].replace("nm", "")
            try:
                person = dict(ia.get_person(imdb_id).items())
            except IMDbParserError as e:
                person = {}
            image_url = person.get("full-size headshot")
            image_url = image_url if image_url is not None else "this is an invalid image url"
        else:
            raise RuntimeError("No reference image url column available in the csv file")

        folder_name = name.replace(" ", "_")
        output_dir = os.path.join(img_root_dir, folder_name)
        argv = ['-e', 'Google', '-d', 'chrome_headless', '-n', f'{IMAGE_RECALL_NUM}', '-j', '10', '-o',
                output_dir, '-F', '-S', f'{name}', '-B', f"{birthdate}"]

        num_img = google_download(argv=argv)
        valid_img = 0
        if num_img > 0:
            candidate_imgs = os.listdir(output_dir)
            reference_img_name = imageDownloader.download(output_dir, image_url, imagename="reference_img")
            if reference_img_name is not None:  # case when the reference image is no longer available thus download fails
                reference_img_path = os.path.join(output_dir, reference_img_name)

                if MULTIPROCESS:
                    executor = ProcessPoolExecutor(max_workers=NUM_WORKERS)
                    futures = [executor.submit(verify, reference_img_path, os.path.join(output_dir, img)) for
                               img in candidate_imgs]
                    for future in as_completed(futures):
                        """wait for all process to finish"""
                        valid_img += future.result()

                else:
                    for img in candidate_imgs:
                        img_path = os.path.join(output_dir, img)
                        result = verify(reference_img_path, img_path)
                        valid_img += result

                _, ext = os.path.splitext(reference_img_name)
                os.rename(reference_img_path, os.path.join(output_dir, "reference_img" + ext))
                if REMOVE_REFERENCE_IMG:
                    os.remove(os.path.join(output_dir, "reference_img", ext))
        if valid_img == 0:
            shutil.rmtree(output_dir)
        log.info(f"valid images = {valid_img}")

        # write to disk in order to resume if necessary
        with open(runtime_query_path, "w", newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',')
            spamwriter.writerow(col_names)
            for row in line_list:
                spamwriter.writerow(row)

    time_identifier: str = get_time_identifier()
    s3Uploader = S3Uploader(path_identifier=time_identifier, dir=config["s3_dir"])

    filepath = shutil.make_archive(base_name="images", format='tar', root_dir=img_root_dir)

    upload_res = s3Uploader.upload_file(input_filename=filepath, remove=True)

    if upload_res == 0:
        if REMOVE_LOCAL_AFTER_UPLOAD:
            "S3uploader only remove the .tar files, here we delete the individual images"
            shutil.rmtree(img_root_dir)

    endTime = int(round(time.time()))

    log.info("Time used {} s".format(endTime - startTime))
