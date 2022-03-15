#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: MingJia
# @Date:   2020-06-17 9:26:36
# @Last Modified by:   MingJia
# @Last Modified time: 2020-06-17 9:26:36
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class GenomeDownloader(object):
    """
    Genome downloader of NCBI
    """

    def __init__(self, out, ascp, ascp_key, ascp_param):
        super(GenomeDownloader, self).__init__()
        self.out = os.path.abspath(out)
        self.set_ascp(ascp)
        self.set_ascp_key(ascp_key)
        self.set_ascp_param(ascp_param)
        self.ascp_pre = "anonftp@ftp.ncbi.nih.gov:"
        self.db = None
        self.og = []
        self.lineage = None

    def set_out(self, out):
        """
        Set the out put dir
        """
        self.out = os.path.abspath(out)
        if not os.path.exists(self.out):
            logging.info("Make output dir...")
            os.makedirs(self.out)

    def set_ascp(self, ascp):
        """
        Set the ascp software
        """
        self.ascp = ascp

    def set_ascp_key(self, ascp_key):
        """
        Set the ascp key
        """
        self.ascp_key = ascp_key

    def set_ascp_param(self, ascp_param):
        """
        Set ascp param
        """
        self.ascp_param = ascp_param

    def set_db(self, db):
        """
        Set the database to use(refseq | genbank)

        :param db: refseq | genbank
        """
        dbs = set(["refseq", "genbank"])
        if db in dbs:
            self.db = db
        else:
            raise ValueError(f"Not support {db}")

    def set_og(self, og):
        """
        The
        """
        self.og = og

    def set_category(self, category):
        """
        """
        self.category = set(category)

    def set_level(self, level):
        """
        """
        self.level = set(level)

    def ascp_download(self, file_path, file_out):
        """
        """
        cmd = f"{self.ascp} -i {self.ascp_key} {self.ascp_param} {file_path} {file_out}"
        logging.debug(cmd)
        try:
            out_bytes = subprocess.check_output(cmd, shell=True)
            return 0
        except subprocess.CalledProcessError as e:
            code = e.returncode  # Return code
            return code

    def get_taxon_dump(self):
        """
        """
        file_path = f"{self.ascp_pre}/pub/taxonomy/taxdump.tar.gz"
        self.taxdump = os.path.join(self.out, "taxdump.tar.gz")
        logging.info(f"Download the {file_path}")
        if self.ascp_download(file_path, self.taxdump) != 0:
            logging.error(f"Fail to download {file_path}")
            sys.exit()
        else:
            logging.info(f"Saved at {self.taxdump}")

    def set_lineage(self, taxonid, file_nodes_dmp=None):
        """

        :param taxonid:
        :param names:
        :return:
        """
        from Lineage import Node
        if file_nodes_dmp is None:
            self.get_taxon_dump()
            cmd = f"tar xf {self.taxdump} -C {os.path.dirname(self.taxdump)} nodes.dmp"
            cmd_list = cmd.strip().split()
            try:
                check_call(cmd_list)
                file_nodes_dmp = os.path.join(os.path.dirname(self.taxdump),
                                              "nodes.dmp")
            except Exception as e:
                logging.error(e)

        node = Node(file_nodes_dmp)
        for taxon in taxonid:
            node.get_lineage(taxon)
        self.lineage = node.lineage

    def get_summary(self, fout):
        """
        Get the assembly_summary.txt file from NCBI
        """
        file_path = f"{self.ascp_pre}/genomes/{self.db}/assembly_summary_{self.db}.txt"
        self.summary = os.path.abspath(fout)
        logging.info(f"Download the {file_path}")
        code = self.ascp_download(file_path, self.summary)
        if code != 0:
            logging.error(f"Fail to download {file_path}")
            sys.exit(code)
        else:
            logging.info(f"Saved at {self.summary}")

    def filter_summary(self):
        """
        Filter the assembly_summary.txt file
        """
        self.filterd_summary = os.path.join(
            self.out, 'filtered_assembly_summary.txt')
        with open(self.summary, 'r') as IN, open(self.filterd_summary,
                                                 'w') as OUT:
            for line in IN:
                if line.startswith('#'):
                    pass
                elif line.startswith("\n"):
                    pass
                else:
                    arr = line.strip().split('\t')
                    if "all" not in self.category:
                        if arr[4] not in self.category:
                            continue
                    if "all" not in self.level:
                        if arr[11] not in self.level:
                            continue
                    if self.lineage and arr[5] not in self.lineage:
                        continue
                    print(*arr, sep='\t', file=OUT)

    def generate_download_url(self, summary=None):
        """

        :return:
        """
        if summary is None:
            summary = self.filterd_summary
        self.download_url = {}
        with open(summary, 'r') as IN:
            for line in IN:
                arr = line.strip().split('\t')
                ftp_url = arr[19]
                if not ftp_url.startswith("ftp://ftp.ncbi.nlm.nih.gov/"):
                    logging.error(f"WRONG FTP URL FOR {arr[0]}: {ftp_url}")
                name_pre = ftp_url.strip().split('/')[-1]
                name = f"{name_pre}_genomic.fna.gz"
                ascp_url = ftp_url.replace(
                    "ftp://ftp.ncbi.nlm.nih.gov", self.ascp_pre) + f"/{name}"
                file_out = os.path.join(self.out, name)
                self.download_url[ascp_url] = file_out

        logging.info(f"{len(self.download_url)} files to download")
        self.file_download_url = os.path.join(self.out, "download.info")
        with open(self.file_download_url, 'w') as OUT:
            for key, value in self.download_url.items():
                print(*[key, value], sep='\t', file=OUT)
        logging.info(f"Saved download_info to {self.file_download_url}")

    def library_download(self, file_download_url=None, thread=1):
        """
        """
        download_info = {}
        if file_download_url is None:
            download_info = self.download_url
        else:
            with open(file_download_url, 'r') as IN:
                for line in IN:
                    arr = line.strip().split('\t')
                    download_info[arr[0]] = arr[1]

        tasks = []
        fail_tasks = []
        with ThreadPoolExecutor(max_workers=thread) as executor:
            for key, value in download_info.items():
                task = executor.submit(self.ascp_download, key, value)
                tasks.append(task)
            for future in as_completed(tasks):
                exec_res = future.result()
                if exec_res != 0:
                    fail_tasks.append(future.result())

        if len(fail_tasks) > 0:
            with open(os.path.join(self.out, "download.fail"), 'w') as OUT:
                for fail_task in fail_tasks:
                    print(*fail_task, sep='\t', file=OUT)
