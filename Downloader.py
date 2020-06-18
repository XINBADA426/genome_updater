#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: MingJia
# @Date:   2020-06-17 9:26:36
# @Last Modified by:   MingJia
# @Last Modified time: 2020-06-17 9:26:36
import logging
import os
import sys
from subprocess import check_call
from subprocess import run

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class GenomeDownloader(object):
    """docstring for GenomeDownloader"""

    def __init__(self):
        super(GenomeDownloader, self).__init__()
        self.ascp = None
        self.ascp_key = None
        self.ascp_param = None
        self.ascp_pre = "anonftp@ftp.ncbi.nih.gov:"

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

    def set_og(self, og):
        """
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
        # cmd = f"{self.ascp} -i {self.ascp_key} {self.ascp_param} {file_path} {file_out}"
        list_cmd = [self.ascp, "-i", self.ascp_key, *
        self.ascp_param.strip().split(' '), file_path, file_out]
        # logging.info(cmd)
        try:
            check_call(list_cmd)
            return 0
        except Exception as e:
            return (file_out, file_path)

    def get_taxon_dump(self):
        """
        """
        file_path = f"{self.ascp_pre}/pub/taxonomy/taxdump.tar.gz"
        self.taxdump = os.path.join(self.out, "taxdump.tar.gz")
        logging.info(f"Download the {file_path}")
        self.ascp_download(file_path, self.taxdump)
        logging.info(f"Saved at {self.taxdump}")

    def get_summary(self):
        """
        Get the summary file
        """
        summary_files = []
        for og in self.og:
            file_path = f"{self.ascp_pre}/genomes/refseq/{og}/assembly_summary.txt"
            summary = os.path.join(self.out, f"{og}_assembly_summary.txt")
            logging.info(f"Download the {file_path}")
            if self.ascp_download(file_path, summary) != 0:
                logging.error(f"Fail to download {file_path}")
                sys.exit()
            else:
                logging.info(f"Saved at {summary}")
            summary_files.append(summary)
        logging.info("Merge the summary files")
        self.summary = os.path.join(self.out, f"assembly_summary.txt")
        cmd = "cat {} > {}".format(' '.join(summary_files), self.summary)
        try:
            run(cmd, shell=True)
            logging.info(f"Saved at {self.summary}")
        except Exception as e:
            sys.exit(e)

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
                    print(*arr, sep='\t', file=OUT)

    def library_download(self, summary=None):
        """
        """
        if summary is None:
            summary = self.filterd_summary
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
                logging.info(f'Download {arr[0]}...')
                self.ascp_download(ascp_url, file_out)
                logging.info(f"Saved {arr[0]} at {file_out}")
