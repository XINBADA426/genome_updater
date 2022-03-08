#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Chaobo Ren
# @Date:   2022/3/8 16:45
# @Last Modified by:   Ming
# @Last Modified time: 2022/3/8 16:45
import logging

import click

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-t', '--taxonid',
              required=True,
              type=click.Path(),
              help="The taxon id of species")
@click.option('-o', '--out',
              required=True,
              type=click.Path(),
              help="The out put dir")
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
def main(taxonid, out, ascp, key, bandwidth, thread):
    """
    Download target sepecies genome from NCBI
    """
    pass


if __name__ == "__main__":
    main()
