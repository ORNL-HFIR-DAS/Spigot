"""
Spigot: SPICE data download tool (HTTP).
"""

import os
import sys
import time
import urllib.parse
from bs4 import BeautifulSoup, SoupStrainer
import concurrent.futures as cf
from requests_futures.sessions import FuturesSession


class SpigotHTTP(object):
    """
    SpigotHTTP downloads SPICE experiment data via HTTP interface.
    """

    def __init__(self, instrument, experiment_number, file_store_base, url_root="http://neutron.ornl.gov/"):
        self.instrument = instrument
        self.experiment_number = experiment_number
        self.file_store_base = file_store_base
        self.download_session = FuturesSession(max_workers=20)
        self.url_root = url_root
        self.all_directory_urls = {}
        self.all_file_urls = {}
        self.overwrite_len = 200

    def _get_file_and_directory_urls_in_html_directories(self, html_directories):
        """
        Retrieve file and directory urls for the given html directories.
        :param html_directories:
        :return:
        """
        # Get directories asynchronously.
        async_downloads = {}

        for url, file_path in html_directories.items():
                file_request = self.download_session.get(url, stream=True)
                async_downloads[file_request] = {'url': url, 'file_path': file_path}

        directory_urls = {}
        file_urls = {}

        for file_request in cf.as_completed(async_downloads, timeout=500000):
            page = file_request.result().content
            url_base = async_downloads[file_request]['url']
            file_path_base = async_downloads[file_request]['file_path']
            for link in BeautifulSoup(page, "html.parser", parse_only=SoupStrainer('a')):
                if link.has_attr('href'):
                    filename = link['href']
                    if filename.startswith("?") or filename.startswith("/"):
                        continue
                    url = url_base.rstrip("/") + "/" + filename
                    file_path = os.path.join(file_path_base, filename.replace("/", ""))
                    if url.endswith("/"):
                        self.all_directory_urls[url] = file_path
                        directory_urls[url] = file_path
                    else:
                        self.all_file_urls[url] = file_path
                        self._print_overwrite(f"Found {len(self.all_file_urls)} files so far ...")
                        file_urls[url] = file_path
        return directory_urls, file_urls

    def _get_some_directories_and_files_async(self, html_directories):
        """
        Recursive function to get directories and files for given base url and base path.
        :type html_directories: object
        """
        # noinspection PyTypeChecker
        new_directory_urls, new_file_urls = self._get_file_and_directory_urls_in_html_directories(html_directories)
        # print(f"Found {len(self.all_file_urls)} files so far ...")
        if len(new_directory_urls) > 0:
            self._get_some_directories_and_files_async(new_directory_urls)

    def _get_directories_and_files_async(self, base_url, local_base_path):
        """
        Recursive function to get directories and files for given base url and base path.
        :param base_url:
        :param local_base_path:
        :return:
        """
        self.all_directory_urls = {}
        self.all_file_urls = {}
        html_directories = {base_url: local_base_path}
        self._get_some_directories_and_files_async(html_directories)
        print(f"\n\nFound a total of {len(self.all_file_urls)} files.")

    def _print_overwrite(self, message):
        print('\r', end='')
        sys.stdout.flush()
        print(' ' * self.overwrite_len, end='')
        sys.stdout.flush()
        print(f'\r{message}', end='')
        self.overwrite_len = len(message)

    def download(self):

        start_time = time.time()

        instrument_path_part = str(self.instrument).replace("-", "").lower()
        experiment_path_part = f"exp{int(self.experiment_number)}"
        url_base = urllib.parse.urljoin(self.url_root, f"user_data/{instrument_path_part}/{experiment_path_part}")
        file_path_base = os.path.join(os.path.join(self.file_store_base, instrument_path_part), experiment_path_part)

        print("\nIdentifying files...")
        self._get_directories_and_files_async(url_base, file_path_base)
        for url, directory_path in self.all_directory_urls.items():
            # Create directories if they don't exist.
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)

        print("\nDownloading files...\n")
        start_download_time = time.time()

        # Download files asynchronously.
        async_downloads = {}

        for url, file_path in self.all_file_urls.items():
                file_request = self.download_session.get(url, stream=True)
                async_downloads[file_request] = file_path

        total_bytes = 0
        file_index = 0
        file_count = len(async_downloads)
        progress_status = f"{file_index}/{file_count} = {0:.2f}%, {float(file_count)/100:.2f} seconds remaining (estimated)"
        file_status = ""
        self._print_overwrite(f"{progress_status} ----> {file_status}")
        for file_request in cf.as_completed(async_downloads, timeout=500000):
            response = file_request.result()

            # Update pre-write status.
            file_path = async_downloads[file_request]
            file_status = f"Writing {file_path}..."
            self._print_overwrite(f"{progress_status} ----> {file_status}")

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    # filter out keep-alive new chunks
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        total_bytes += len(chunk)

            # Update post-write status.
            file_index += 1
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

        elapsed_time = time.time() - start_time
        mega_bytes = float(total_bytes)/(1024 * 1024)
        print(f"\n\nDownload complete for Instrument {self.instrument} Experiment {self.experiment_number} ("
              f"{file_count} files, {mega_bytes:.3f} MB, {elapsed_time:.2f} seconds,"
              f" {mega_bytes/elapsed_time:.3f} MB/sec).")
