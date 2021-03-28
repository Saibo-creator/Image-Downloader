import copy
import shutil
import os
import time
from image_downloader import google_download
import csv
import logging
from ImageLabelingPackage.ImageDownloadLabeler import ImageDownloader
from deepface import DeepFace
from PyS3Upload.PyS3Uploader import S3Uploader
from PyS3Upload.helper import get_time_identifier

if __name__ == '__main__':

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    IMAGE_RECALL_NUM = 50
    REMOVE_REFERENCE_IMG = False
    REMOVE_LOCAL_AFTER_UPLOAD = True

    startTime = int(round(time.time()))

    line_list = []
    query_fn = 'query_2000_2020_6147.csv'
    # runtime_query_fn = f'{query_fn.split(".")[0]}_runtime.csv'

    with open('query_results/has_img/query_2000_2020_6147_offset708.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            line_list.append(row)

    imageDownloader = ImageDownloader()
    img_root_dir = 'img/google/kids_actor_11_no_safe'
    while len(line_list) > 0:
        line = line_list.pop(0)
        search_pair = (line[0], line[2], line[3])
        folder_name = search_pair[0].replace(" ", "_")
        name = search_pair[0]
        birthdate = search_pair[1]
        image_url = search_pair[2]
        output_dir = os.path.join(img_root_dir, folder_name)
        argv = ['-e', 'Google', '-d', 'chrome_headless', '-n', f'{IMAGE_RECALL_NUM}', '-j', '10', '-o',
                output_dir, '-F', '-S', f'{name}', '-B', f"{birthdate}"]

        num_img = google_download(argv=argv)
        valid_img = 0
        if num_img > 0:
            candidate_imgs = os.listdir(output_dir)
            reference_img_name = imageDownloader.download(output_dir, image_url)
            reference_img_path = os.path.join(output_dir, reference_img_name)
            for img in candidate_imgs:
                img_path = os.path.join(output_dir, img)
                result = DeepFace.verify(reference_img_path, img_path, enforce_detection=False, model_name='VGG-Face')
                if not result["verified"]:
                    os.remove(img_path)
                else:
                    valid_img += 1
            _, ext = os.path.splitext(reference_img_name)
            os.rename(reference_img_path, os.path.join(output_dir, "reference_img" + ext))
            if REMOVE_REFERENCE_IMG:
                os.remove(os.path.join(output_dir, "reference_img", ext))

        if valid_img == 0:
            shutil.rmtree(output_dir)
        log.info(f"valid images = {valid_img}")

    time_identifier: str = get_time_identifier()
    s3Uploader = S3Uploader(path_identifier=time_identifier, dir="margin_project/KidFace")

    filepath = shutil.make_archive(base_name="images", format='tar', root_dir=img_root_dir)

    upload_res = s3Uploader.upload_file(input_filename=filepath, remove=True)

    if upload_res == 0:
        if REMOVE_LOCAL_AFTER_UPLOAD:
            "S3uploader only remove the .tar files, here we delete the individual images"
            shutil.rmtree(img_root_dir)

    endTime = int(round(time.time()))

    log.info("Time used {} s".format(endTime - startTime))
