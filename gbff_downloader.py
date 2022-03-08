#!/Bio/User/renchaobo/software/miniconda3/bin/python
# -*- coding: utf-8 -*-
# @Author: MingJia
# @Date:   2020-06-15 16:41:39
# @Last Modified by:   MingJia
# @Last Modified time: 2020-06-16 16:00:38
import logging
import os

import click

from lib.Downloader import GenomeDownloader

#### Some Functions ####
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def parse_taxon(file_name):
    """

    :param file_name:
    :return:
    """
    res = set()
    with open(file_name, 'r') as IN:
        for line in IN:
            res.add(line.strip())
    return res


class GbffDownloader(GenomeDownloader):
    """
    """

    def __init__(self):
        """
        """
        super(GbffDownloader, self).__init__()

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
                name = f"{name_pre}_genomic.gbff.gz"
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


########################


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--db',
              default="refseq",
              show_default=True,
              type=click.Choice(
                  ["refseq", "genbank"]),
              help="The database to use")
@click.option('--og',
              required=True,
              multiple=True,
              type=click.Choice(
                  ["all", "archaea", "bacteria", "fungi", "viral"]),
              help="The Organism group to downloads")
@click.option('--taxon',
              type=click.Path(),
              help="The file contain taxon id you want to download")
@click.option('-c', '--category',
              multiple=True,
              default=["all"],
              type=click.Choice(
                  ["all", "reference genome", "representative genome", "na"]),
              show_default=True,
              help="Assembly level")
@click.option('-l', '--level',
              multiple=True,
              default=["all"],
              type=click.Choice(
                  ["all", "Complete Genome", "Chromosome", "Scaffold",
                   "Contig"]),
              show_default=True,
              help="Assembly level")
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
def cli(db, og, taxon, category, level, ascp, key, bandwidth, thread, out):
    """
    Update the refseq genome sequence

    """

    # Init the GenomeDownloader
    downloader = GbffDownloader()

    # set outputdir
    downloader.set_out(out)
    # set ascp param
    downloader.set_ascp(ascp)
    downloader.set_ascp_key(key)
    ascp_param = f"-k 1 -T -l{bandwidth}"
    downloader.set_ascp_param(ascp_param)
    # set download info
    downloader.set_db(db)
    downloader.set_og(og)
    downloader.set_category(category)
    downloader.set_level(level)
    if taxon:
        downloader.set_lineage(parse_taxon(taxon))

    # Start to deal the info
    downloader.get_summary()
    # filter summary
    downloader.filter_summary()
    # get download url
    downloader.generate_download_url()
    # download library
    downloader.library_download(thread=thread)


if __name__ == "__main__":
    cli()
