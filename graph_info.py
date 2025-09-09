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
# tree.assign_generation_levels(debug=True)

# Print out all people with their details in a table with fixed witdth columns
print(f"{'ID':36} {'First Name':20} {'Last Name':20} {'Birth Year':10} {'Birthplace':20} {'Profile pic (Y/N)':20}")
for person_id in tree.graph.nodes:
    person = tree.graph.nodes[person_id]
    if 'profilepic' in person and person['profilepic']:
        profilepic_exists = 'Y'
    else:
        profilepic_exists = 'N'
    print(f"{person_id:36} {person.get('firstname', ''):20} {person.get('lastname', ''):20} {person.get('birthyear', ''):10} {person.get('birthplace', ''):20} {profilepic_exists:20}")

# Perform any modifications to the tree here
# for person_id in tree.graph.nodes:
#     person = tree.graph.nodes[person_id]

# Save the updated tree back to Azure Storage
#tree.save()