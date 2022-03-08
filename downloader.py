#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Chaobo Ren
# @Date:   2022/3/8 17:03
# @Last Modified by:   Ming
# @Last Modified time: 2022/3/8 17:03
import logging

import click

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--ascp',
              default="ascp",
              type=click.Path(),
              show_default=True,
              help="The ascp path")
@click.option('--key',
              default="~/.aspera/connect/etc/asperaweb_id_dsa.openssh",
              type=click.Path(),
              show_default=True,
              help="The ascp key")
@click.option('--bandwidth',
              default="10m",
              show_default=True,
              help="The bandwidth for ascp")
def cli():
    """
    Genome Downloader for NCBI Genomes
    """
    pass


@click.command()
@click.option('-o', '--out',
              required=True,
              type=click.Path(),
              help="The out put dir")
def summary(out):
    """
    Download the assembly_summary_{self.db}.txt file from NCBI
    """
    pass


if __name__ == "__main__":
    main()
