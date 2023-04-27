import os

def print_directory_contents(path):
    """
    This function prints out all the files and directories
    within a given path, including sub-directories.
    It skips directories that the user does not have permission to access.
    """
    try:
        # Check if the user has permission to access the directory
        os.listdir(path)
    except PermissionError:
        # If no permission, skip this directory and return
        return
    
    # Loop through all items in the directory
    for child in os.listdir(path):
        child_path = os.path.join(path, child)
        if os.path.isdir(child_path):
            # If it's a directory, recursively call print_directory_contents
            print_directory_contents(child_path)
        else:
            # Otherwise, it's a file, so print its path
            print(child_path)

print_directory_contents("d:\\")