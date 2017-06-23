"""
Spigot: SPICE data download tool (HTTP).
"""


import sys
import paramiko
import os
import getpass
import time
from stat import S_ISDIR

# paramiko.util.log_to_file('E:\\Temp\\paramiko.log')


class SpigotSFTP(object):
    """
    SpigotSFTP downloads SPICE experiment data via SFTP interface.
    """

    def __init__(self, instrument, ipts_number, experiment_number, local_path_base,
                 user=None, host="analysis.sns.gov", remote_path_base="/HFIR", sftp_port=22):
        self.instrument = instrument
        # Handle IPTS numbers like 12345.7, 'IPTS-0003', and 'IPTS 00455'.
        self.ipts_number = int(float(str(ipts_number).upper().replace("IPTS", "").replace("-", "")))
        self.experiment_number = int(experiment_number)
        self.local_path_base = local_path_base
        self.user = user
        self.host = host
        self.remote_path_base = remote_path_base
        self.sftp_port = sftp_port
        self.all_directories = {}
        self.all_files = {}
        self.overwrite_len = 200
        remote_path_suffix = self.instrument.upper().replace("-", "") + "/" + f"IPTS-{self.ipts_number:04}" + f"/exp{self.experiment_number}"
        self.remote_path_initial = self.remote_path_base.rstrip("/") + "/" + remote_path_suffix
        self.local_path_initial = os.path.join(self.local_path_base, remote_path_suffix.replace("/", os.path.sep))
        self.sftp_client = None

    def _print_overwrite(self, message):
        print('\r', end='')
        sys.stdout.flush()
        print(' ' * self.overwrite_len, end='')
        sys.stdout.flush()
        print(f'\r{message}', end='')
        self.overwrite_len = len(message)

    def _get_local_path_from_remote_path(self, remote_path):
        new_part_of_path = remote_path.replace(self.remote_path_initial, "").strip("/")
        local_path = os.path.join(self.local_path_initial, new_part_of_path.replace("/", os.path.sep))
        return local_path

    def _register_directory(self, remote_directory_path):
        local_path = self._get_local_path_from_remote_path(remote_directory_path)
        self.all_directories[remote_directory_path] = local_path

    def _register_file(self, remote_file_path):
        local_path = self._get_local_path_from_remote_path(remote_file_path)
        self.all_files[remote_file_path] = local_path
        self._print_overwrite(f"Found {len(self.all_files)} files so far ...")

    def _assure_local_directory_exists(self, remote_directory_path):
        directory = self._get_local_path_from_remote_path(remote_directory_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _find_directories_and_files(self, remote_path):
        folder_paths = []
        for f in self.sftp_client.listdir_attr(remote_path):
            if S_ISDIR(f.st_mode):
                folder_path = remote_path.rstrip("/") + "/" + f.filename
                self._register_directory(folder_path)
                folder_paths.append(folder_path)
            else:
                file_path = remote_path.rstrip("/") + "/" + f.filename
                self._register_file(file_path)
        for folder_path in folder_paths:
            self._find_directories_and_files(folder_path)

    def _assure_all_local_directories_exist(self):
        self._assure_local_directory_exists(self.local_path_initial)
        for remote_path in self.all_directories.keys():
            self._assure_local_directory_exists(remote_path)

    def download(self, password=None):
        start_time = time.time()
        print("\nIdentifying files...")
        # noinspection PyTypeChecker
        transport = paramiko.Transport((self.host, self.sftp_port))
        if not password:
            password = getpass.getpass(prompt=f"Password for {self.user} on {self.host}: ", stream=sys.stderr)
        transport.connect(username=self.user, password=password)
        self.sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._find_directories_and_files(self.remote_path_initial)
        print(f"\n\nFound a total of {len(self.all_files)} files.")

        self._assure_all_local_directories_exist()

        print("\nDownloading files...\n")
        start_download_time = time.time()

        total_bytes = 0
        file_index = 0
        file_count = len(self.all_files)
        progress_status = f"{file_index}/{file_count} = {0:.2f}%, {float(file_count)/100:.2f} seconds remaining (estimated)"
        file_status = ""
        self._print_overwrite(f"{progress_status} ----> {file_status}")
        for remote_path, local_path in self.all_files.items():
            # Update pre-write status.
            file_status = f"Writing {local_path}..."
            self._print_overwrite(f"{progress_status} ----> {file_status}")

            self.sftp_client.get(remote_path, local_path)

            # Update post-write status.
            file_index += 1
            total_bytes += os.path.getsize(local_path)
            elapsed_download_time = time.time() - start_download_time
            fraction_complete = float(file_index)/file_count
            if fraction_complete > 0:
                time_remaining_estimate = elapsed_download_time * (1 - fraction_complete) / fraction_complete
            else:
                # No completions yet. So just estimate 60 files/second.
                time_remaining_estimate = float(file_count)/100
            progress_status = f"{file_index}/{file_count} = {fraction_complete * 100:.2f}%, " \
                              f"{time_remaining_estimate:.2f} seconds remaining (estimated)"
            file_status += "complete."
            self._print_overwrite(f"{progress_status} ----> {file_status}")

        self.sftp_client.close()
        elapsed_time = time.time() - start_time
        mega_bytes = float(total_bytes)/(1024 * 1024)
        print(f"\n\nDownload complete for Instrument {self.instrument.upper()} IPTS-{self.ipts_number:04} Experiment {self.experiment_number} ("
              f"{file_count} files, {mega_bytes:.3f} MB, {elapsed_time:.2f} seconds,"
              f" {mega_bytes/elapsed_time:.3f} MB/sec).")

# class SpigotSFTP(object):
#     """
#     SpigotSFTP downloads SPICE experiment data via SFTP interface.
#     """
#
#     def __init__(self, instrument, ipts_number, experiment_number, file_store_base, url_root="http://neutron.ornl.gov/"):
#         self.instrument = instrument
#         self.experiment_number = experiment_number
#         self.file_store_base = file_store_base
#         self.download_session = FuturesSession(max_workers=20)
#         self.url_root = url_root
#         self.all_directory_urls = {}
#         self.all_file_urls = {}
#         self.overwrite_len = 200
#
#     def _get_file_and_directory_urls_in_html_directories(self, html_directories):
#         """
#         Retrieve file and directory urls for the given html directories.
#         :param html_directories:
#         :return:
#         """
#         # Get directories asynchronously.
#         async_downloads = {}
#
#         for url, file_path in html_directories.items():
#                 file_request = self.download_session.get(url, stream=True)
#                 async_downloads[file_request] = {'url': url, 'file_path': file_path}
#
#         directory_urls = {}
#         file_urls = {}
#
#         for file_request in cf.as_completed(async_downloads, timeout=500000):
#             page = file_request.result().content
#             url_base = async_downloads[file_request]['url']
#             file_path_base = async_downloads[file_request]['file_path']
#             for link in BeautifulSoup(page, "html.parser", parse_only=SoupStrainer('a')):
#                 if link.has_attr('href'):
#                     filename = link['href']
#                     if filename.startswith("?") or filename.startswith("/"):
#                         continue
#                     url = url_base.rstrip("/") + "/" + filename
#                     file_path = os.path.join(file_path_base, filename.replace("/", ""))
#                     if url.endswith("/"):
#                         self.all_directory_urls[url] = file_path
#                         directory_urls[url] = file_path
#                     else:
#                         self.all_file_urls[url] = file_path
#                         self._print_overwrite(f"Found {len(self.all_file_urls)} files so far ...")
#                         file_urls[url] = file_path
#         return directory_urls, file_urls
#
#     def _get_some_directories_and_files_async(self, html_directories):
#         """
#         Recursive function to get directories and files for given base url and base path.
#         :type html_directories: object
#         """
#         # noinspection PyTypeChecker
#         new_directory_urls, new_file_urls = self._get_file_and_directory_urls_in_html_directories(html_directories)
#         # print(f"Found {len(self.all_file_urls)} files so far ...")
#         if len(new_directory_urls) > 0:
#             self._get_some_directories_and_files_async(new_directory_urls)
#
#     def _get_directories_and_files_async(self, base_url, local_base_path):
#         """
#         Recursive function to get directories and files for given base url and base path.
#         :param base_url:
#         :param local_base_path:
#         :return:
#         """
#         self.all_directory_urls = {}
#         self.all_file_urls = {}
#         html_directories = {base_url: local_base_path}
#         self._get_some_directories_and_files_async(html_directories)
#         print(f"\n\nFound a total of {len(self.all_file_urls)} files.")
#
#     def _print_overwrite(self, message):
#         print('\r', end='')
#         sys.stdout.flush()
#         print(' ' * self.overwrite_len, end='')
#         sys.stdout.flush()
#         print(f'\r{message}', end='')
#         self.overwrite_len = len(message)
#
#     def download(self):
#
#         start_time = time.time()
#
#         instrument_path_part = str(self.instrument).replace("-", "").lower()
#         experiment_path_part = f"exp{int(self.experiment_number)}"
#         url_base = urllib.parse.urljoin(self.url_root, f"user_data/{instrument_path_part}/{experiment_path_part}")
#         file_path_base = os.path.join(os.path.join(self.file_store_base, instrument_path_part), experiment_path_part)
#
#         print("\nIdentifying files...")
#         self._get_directories_and_files_async(url_base, file_path_base)
#         for url, directory_path in self.all_directory_urls.items():
#             # Create directories if they don't exist.
#             if not os.path.exists(directory_path):
#                 os.makedirs(directory_path)
#
#         print("\nDownloading files...\n")
#         start_download_time = time.time()
#
#         # Download files asynchronously.
#         async_downloads = {}
#
#         for url, file_path in self.all_file_urls.items():
#                 file_request = self.download_session.get(url, stream=True)
#                 async_downloads[file_request] = file_path
#
#         total_bytes = 0
#         file_index = 0
#         file_count = len(async_downloads)
#         progress_status = f"{file_index}/{file_count} = {0:.2f}%, {float(file_count)/100:.2f} seconds remaining (estimated)"
#         file_status = ""
#         self._print_overwrite(f"{progress_status} ----> {file_status}")
#         for file_request in cf.as_completed(async_downloads, timeout=500000):
#             response = file_request.result()
#             file_path = async_downloads[file_request]
#             file_status = f"Writing {file_path}..."
#             self._print_overwrite(f"{progress_status} ----> {file_status}")
#             with open(file_path, 'wb') as f:
#                 for chunk in response.iter_content(chunk_size=8192):
#                     # filter out keep-alive new chunks
#                     if chunk:
#                         f.write(chunk)
#                         f.flush()
#                         total_bytes += len(chunk)
#             file_index += 1
#             elapsed_download_time = time.time() - start_download_time
#             fraction_complete = float(file_index)/file_count
#             if fraction_complete > 0:
#                 time_remaining_estimate = elapsed_download_time * (1 - fraction_complete) / fraction_complete
#             else:
#                 # No completions yet. So just estimate 60 files/second.
#                 time_remaining_estimate = float(file_count)/100
#             progress_status = f"{file_index}/{file_count} = {fraction_complete * 100:.2f}%, " \
#                               f"{time_remaining_estimate:.2f} seconds remaining (estimated)"
#             file_status += "complete."
#             self._print_overwrite(f"{progress_status} ----> {file_status}")
#         elapsed_time = time.time() - start_time
#         mega_bytes = float(total_bytes)/1000000
#         print(f"\n\nDownload complete for Instrument {self.instrument} Experiment {self.experiment_number} ("
#               f"{file_count} files, {mega_bytes:.3f} MB, {elapsed_time:.2f} seconds,"
#               f" {mega_bytes/elapsed_time:.3f} MB/sec).")
