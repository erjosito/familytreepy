# Used to test hi-res image generation of family tree

from PIL import Image, ImageDraw, ImageFont, ImageFilter
import numpy as np
import requests
import tempfile
import os
from familytree import FamilyTree
import random
import argparse

# Get azure storage credentials (account name and key) and container/blob names from command line arguments
parser = argparse.ArgumentParser(description='Family Tree editing  with Azure Storage Backend')
parser.add_argument('--azure-storage-account', type=str, required=True, help='Azure Storage Account Name', dest='azure_storage_account')
parser.add_argument('--azure-storage-key', type=str, required=True, help='Azure Storage Account Key', dest='azure_storage_key')
parser.add_argument('--azure-storage-container', type=str, required=True, help='Azure Storage Container Name', dest='azure_storage_container')
parser.add_argument('--azure-storage-blob', type=str, required=True, help='Azure Storage Blob Name', dest='azure_storage_blob')
parser.add_argument('--azure-storage-sas', type=str, required=True, help='Azure Storage SAS Token', dest='azure_storage_sas')
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
args = parser.parse_args()

# Load up tree from Azure Storage
tree = FamilyTree(backend='azstorage', azstorage_account=args.azure_storage_account, azstorage_key=args.azure_storage_key, azstorage_container=args.azure_storage_container, azstorage_blob=args.azure_storage_blob, verbose=args.verbose)
root_person_id = "4fa15fba-3fea-4f95-a947-3c165de9aed6"
degrees = 3

# Generate image
temp_folder = tempfile.TemporaryDirectory()
tree.generate_image(
    root_person_id=root_person_id, degrees=degrees, 
    canvas_width=1600, canvas_height=1200, 
    root_folder='./imagegen', image_path=temp_folder.name, image_filename='familytree.png', 
    azure_storage_sas=args.azure_storage_sas,
    verbose=args.verbose)
im = Image.open(os.path.join(temp_folder.name, 'familytree.png'))
im.show()
