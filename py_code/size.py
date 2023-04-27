import os

def get_directory_size(path):
    """
    This function returns the total size, in bytes, of all files
    and subdirectories within a given path.
    """
    total_size = 0
    
    # Loop through all items in the directory
    for child in os.listdir(path):
        child_path = os.path.join(path, child)
        if os.path.isdir(child_path):
            # If it's a directory, recursively call get_directory_size
            total_size += get_directory_size(child_path)
        else:
            # Otherwise, it's a file, so add its size to the total
            total_size += os.path.getsize(child_path)
    
    return total_size

# Get the sizes of all subdirectories and sort them by size
descending_sizes = sorted([(get_directory_size(os.path.join('f:\\code', child)), child) for child in os.listdir('f:\\code') if os.path.isdir(os.path.join('f:\\code', child))], reverse=True)

# Print the subdirectory names and their sizes
for size, name in descending_sizes:
    print(name + ': ' + str(size))
