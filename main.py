from sql_operation.db_operate import create_table
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--time_stamp", type=str)
    # For "ip_to_domain" mode file is required to give ip to domain mapping, for "get_ptr" mode file is required to give ip address
    parser.add_argument("--mode", type=str, help="ip_to_domain, get_ptr, get_webpage, organization_identification, get_wiki_info, label_wiki")
    parser.add_argument("--hitlist", type=str)
    parser.add_argument("--pyasn_file", type=str)
    parser.add_argument("--ptr_file", type=str)
    parser.add_argument("--label_wiki_model", type=str)
    parser.add_argument("--combine_label_model", type=str)
    parser.add_argument("--copy_cluster_from", type=str)
    args = parser.parse_args()
    if args.time_stamp is None:
        print("Time stamp is required")
        exit(1)
    time_stamp = args.time_stamp
    mode = args.mode
    if mode == "get_ptr":
        if args.hitlist is None:
            print("Hitlist is required for get_ptr mode")
            exit(1)
        if args.pyasn_file is None:
            print("ASN file is required for get_ptr mode")
            exit(1)
        create_table(time_stamp)
        from get_domain.get_ptr import ip_to_ptr
        ip_to_ptr(args.hitlist, args.pyasn_file, time_stamp)
    elif mode == "ip_to_domain":
        if args.ptr_file is None:
            print("PTR Record file is required for ip_to_domain mode")
            exit(1)
        if args.pyasn_file is None:
            print("ASN file is required for ip_to_domain mode")
            exit(1)
        create_table(time_stamp)
        from get_domain.ip_to_domain import insert_ip_url
        insert_ip_url(args.ptr_file, args.pyasn_file, time_stamp)
    elif mode == "copy_cluster":
        if args.copy_cluster_from is None:
            print("Copy cluster from is required for copy_cluster mode")
            exit(1)
        from organization_cluster.cluster_utils import copy_cluster
        copy_cluster(time_stamp, args.copy_cluster_from)
    elif mode == "whois":
        from organization_cluster.org_identification import organization_identification_by_whois
        organization_identification_by_whois(time_stamp)
        from scraper.get_wiki_url_request import get_wiki_info
        get_wiki_info(time_stamp)
        from organization_cluster.cluster_utils import clean_cluster
        clean_cluster(time_stamp)
    elif mode == "cert":
        from scraper.get_cert import get_cert
        from organization_cluster.org_identification import organization_identification_by_cert
        get_cert(time_stamp)
        organization_identification_by_cert(time_stamp)
    elif mode == "get_webpage":
        from scraper.get_webpage import get_webpage
        get_webpage(time_stamp)
    elif mode == "get_wiki_info":
        from scraper.get_wiki_url_request import get_wiki_info
        get_wiki_info(time_stamp)
    elif mode == "label_wiki":
        if args.label_wiki_model is None:
            print("Label wiki model is required for label_wiki mode, e.g. --label_wiki_model gpt,gemini,...,combine")
            exit(1)
        from llm_label import llm_label
        models = args.label_wiki_model.split(",")
        models = [model.strip() for model in models]
        if args.combine_label_model is not None:
            llm_label(time_stamp, models, model_path=args.combine_label_model)
        else:
            llm_label(time_stamp, models)
    elif mode == "label_as":
        from label_as.get_as_domain import label_as
        label_as(time_stamp)
        from label_as.label_as_industry import label_as_industry
        label_as_industry(time_stamp)
    else:
        print("Invalid mode")
        exit(1)

