import os

def list_files(directory):
    try:
        files = os.listdir(directory)
        print("Files in directory:")
        for filename in files:
            print(filename)
        
        # Check for the file specified by the environment variable
        file_name = os.getenv('FILE_NAME')
        if file_name in files:
            print(f"The file '{file_name}' exists in the directory.")
        else:
            print(f"The file '{file_name}' does NOT exist in the directory.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    directory = 'testFolder'  # Change this to your desired folder path
    list_files(directory)