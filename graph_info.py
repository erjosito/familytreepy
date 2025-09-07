from familytree import FamilyTree
import argparse

# Get azure storage credentials (account name and key) and container/blob names from command line arguments
parser = argparse.ArgumentParser(description='Family Tree editing  with Azure Storage Backend')
parser.add_argument('--azure-storage-account', type=str, required=True, help='Azure Storage Account Name', dest='azure_storage_account')
parser.add_argument('--azure-storage-key', type=str, required=True, help='Azure Storage Account Key', dest='azure_storage_key')
parser.add_argument('--azure-storage-container', type=str, required=True, help='Azure Storage Container Name', dest='azure_storage_container')
parser.add_argument('--azure-storage-blob', type=str, required=True, help='Azure Storage Blob Name', dest='azure_storage_blob')
args = parser.parse_args()

# Load up tree from Azure Storage
tree = FamilyTree(backend='azstorage', azstorage_account=args.azure_storage_account, azstorage_key=args.azure_storage_key, azstorage_container=args.azure_storage_container, azstorage_blob=args.azure_storage_blob)

# Show some basic info about the tree
print(f"Family tree loaded from Azure Storage container '{args.azure_storage_container}', blob '{args.azure_storage_blob}'")
print(f"Number of people in the tree: {len(tree.get_person_list())}")
# Longest chain of ancestors
longest_chain = tree.get_longest_ancestor_chain()
print(f"Longest chain of ancestors: {longest_chain}")

# Test setting levels
tree.assign_generation_levels(debug=True)

# Perform any modifications to the tree here

# Save the updated tree back to Azure Storage
#tree.save()