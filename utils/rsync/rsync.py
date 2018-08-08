# -*- coding: UTF-8 -*-

from utils import Configuration, parse_conf_args, rshell


def rsync(host_config, sync_items, base_type=None, base_source=None, base_target=None):
    rsh = rshell(host_config)
    rsh.connect()

    for item in sync_items:
        if base_target is not None and isinstance(item, basestring):
            if base_type == "put":
                rsh.put(item, base_target)
            else:
                rsh.get(item, base_target)
        else:
            sync_type = item.get("type", base_type)
            source_str = item.get("source", base_source)
            target_dir_str = item.get("target", base_target)
            if sync_type == "put":
                rsh.put(source_str, target_dir_str)
            else:
                rsh.get(source_str, target_dir_str)

    rsh.disconnect()


def rsync_groups(context, conf):
    hosts_config = context.get("hosts")

    base_type = conf.get("type")
    base_source = conf.get("source")
    base_target = conf.get("target")
    sync_config = conf.get("Syncs")

    for group in sync_config:
        host_id = group.get("host")
        sync_base_type = group.get("type", base_type)
        sync_base_source = group.get("source", base_source)
        sync_base_target = group.get("target", base_target)
        host_config = hosts_config.get(host_id)

        sync_items = group.get("items")

        rsync(host_config, sync_items, sync_base_type, sync_base_source, sync_base_target)
