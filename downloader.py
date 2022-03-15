#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Chaobo Ren
# @Date:   2022/3/8 17:03
# @Last Modified by:   Ming
# @Last Modified time: 2022/3/8 17:03
import logging
import os.path

import click

from lib.Downloader import GenomeDownloader

logging.basicConfig(level=logging.DEBUG,
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
              default="50m",
              show_default=True,
              help="The bandwidth for ascp")
@click.pass_context
def cli(ctx, ascp, key, bandwidth):
    """
    Genome Downloader for NCBI Genomes
    """
    if ctx.obj is None:
        ctx.obj = dict()
    # ctx.obj['a'] = 'example_A'
    # ctx.obj['b'] = 'example_B'
    ctx.obj["ascp"] = ascp
    ctx.obj["key"] = key
    ctx.obj["bandwidth"] = bandwidth
    # ctx.obj = {"ascp": ascp,
    #            "key": key,
    #            "bandwidth": bandwidth}


@click.command()
@click.pass_context
@click.option('-db', '--database',
              default="refseq",
              show_default=True,
              type=click.Choice(["refseq", "genbank"]),
              help="The database to use")
@click.option('-o', '--out',
              default="assembly_summary.txt",
              show_default=True,
              type=click.Path(),
              help="The out put file")
def summary(ctx, database, out):
    """
    Download the assembly_summary.txt file from NCBI
    """
    out = os.path.abspath(out)
    ascp_param = f"-k 1 -T -l{ctx.obj['bandwidth']}"
    downloader = GenomeDownloader(out, ctx.obj["ascp"], ascp_key=ctx.obj["key"], ascp_param=ascp_param)
    downloader.set_db(database)
    logging.info(f"Start to download")
    f_out = os.path.abspath(out)
    downloader.get_summary(fout=f_out)


cli.add_command(summary)

if __name__ == "__main__":
    cli()
