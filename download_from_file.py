#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: MingJia
# @Date:   2020-06-18 11:23:05
# @Last Modified by:   MingJia
# @Last Modified time: 2020-06-18 11:23:05
import logging
import os

import click

from lib.Downloader import GenomeDownloader

#### Some Functions ####
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def deal_download_file(file_in, file_out):
    """
    Get the download info

    File input file must have the ascp url as the first col, the second
    column is optional, if not be offered, it will download at the
    current dir

    :param file_in: Download url file
    :param file_out: The format Download url file
    :return:
    """
    logging.info(f"Format the {file_in} to {file_out}")
    with open(file_in, 'r') as IN, open(file_out, 'w') as OUT:
        for line in IN:
            arr = line.strip().split('\t')
            if len(arr) == 1:
                print(*[arr[0], '.'], sep='\t', file=OUT)
            else:
                print(*arr[:2], sep='\t', file=OUT)


########################
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-i', '--input',
              required=True,
              type=click.Path(),
              help="The file contain the download info")
@click.option('--ascp',
              default="/export/home/lianglili/.aspera/connect/bin/ascp",
              type=click.Path(),
              show_default=True,
              help="The ascp path")
@click.option('--key',
              default="/export/home/lianglili/.aspera/connect/etc/asperaweb_id_dsa.openssh",
              type=click.Path(),
              show_default=True,
              help="The ascp key")
@click.option('--bandwidth',
              default="10m",
              show_default=True,
              help="The bandwidth for ascp")
@click.option('-t', '--thread',
              default=1,
              show_default=True,
              type=int,
              help="The thread to use")
@click.option('-o', '--out',
              required=True,
              type=click.Path(),
              help="The out put dir")
def cli(input, ascp, key, bandwidth, thread, out):
    """
    Download the file with ascp from ncbi
    """
    # init
    downloader = GenomeDownloader()
    # set outputdir
    downloader.set_out(out)
    # set ascp param
    downloader.set_ascp(ascp)
    downloader.set_ascp_key(key)
    ascp_param = f"-k 1 -T -l{bandwidth}"
    downloader.set_ascp_param(ascp_param)

    # get the down load info
    file_download_url = os.path.join(downloader.out, "download.info")
    deal_download_file(input, file_download_url)
    # download
    logging.info(f"Start to download...")
    downloader.library_download(file_download_url=file_download_url,
                                thread=thread)


if __name__ == "__main__":
    cli()
