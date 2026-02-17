"""Recommendation text templates for index architecture rules.

Each entry maps a recommendation name to its static metadata and
text templates.  Templates use string.Template syntax ($var) so
that JSON examples with literal { } braces need no escaping.

Dynamic values are substituted at runtime by _build_rec() in
index_arch.py.  Plain strings (no $var) are used where the text
is fully static.
"""

from string import Template

TEMPLATES = {

    # ------------------------------------------------------------------
    # Rule 1
    # ------------------------------------------------------------------
    "shard_too_small": {
        "category": "shard_sizing",
        "severity": "warning",
        "why": Template(
            "This index group has $shard_count primary shards averaging "
            "$avg_str each. Elasticsearch recommends primary shards between "
            "10GB and 50GB. Small shards increase cluster overhead — each "
            "shard consumes memory for segment metadata, Lucene instances, "
            "and thread pool slots regardless of data size. Many small shards "
            "also increase query latency due to per-shard coordination "
            "overhead.\n\n"
            "If not addressed: cluster state grows unnecessarily, master node "
            "is under more pressure, and heap usage increases linearly with "
            "shard count."
        ),
        "how": (
            "Option 1 — Reduce shard count in the index template:\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "settings": { "number_of_shards": 1 } } }\n\n'
            "Option 2 — If using ILM with rollover, increase the rollover "
            "max_primary_shard_size threshold so each index accumulates "
            "more data before rolling.\n\n"
            "Option 3 — For time-series indices, consider less frequent "
            "rollover (e.g., weekly instead of daily).\n\n"
            "Changes only affect new indices — existing small-shard indices "
            "are unchanged. Use the Shrink API to consolidate existing indices."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/size-your-shards.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 2
    # ------------------------------------------------------------------
    "shard_too_large": {
        "category": "shard_sizing",
        "severity": "warning",
        "why": Template(
            "Primary shards average $avg_str, exceeding the recommended "
            "50GB maximum. Large shards cause slow recovery — when a node "
            "fails, the entire shard must be copied to another node, which "
            "can take hours for shards over 100GB. Search performance also "
            "suffers because each search runs on a single thread per shard — "
            "a giant shard cannot be parallelized. Force-merge and reindex "
            "operations become very slow and I/O intensive.\n\n"
            "If not addressed: node failures cause extended recovery times, "
            "rolling restarts take much longer, and reindexing becomes "
            "impractical."
        ),
        "how": (
            "Option 1 — Enable ILM rollover with a size threshold:\n\n"
            "  PUT _ilm/policy/<policy-name>\n"
            '  { "policy": { "phases": { "hot": { "actions": {\n'
            '    "rollover": { "max_primary_shard_size": "50gb" }\n'
            "  } } } } }\n\n"
            "Option 2 — Increase number_of_shards in the index template "
            "to spread data across more (smaller) shards.\n\n"
            "Option 3 — Use the Split API to split existing indices:\n\n"
            "  POST /<index>/_split/<target-index>\n"
            '  { "settings": { "index.number_of_shards": 2 } }\n\n'
            "Note: The index must be read-only before splitting."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/size-your-shards.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 3
    # ------------------------------------------------------------------
    "replica_risk": {
        "category": "settings_audit",
        "severity": "warning",
        "why": (
            "This index group has zero replicas. If a data node fails, any "
            "shards on that node are lost until the node recovers. With 0 "
            "replicas, there is no redundant copy — a disk failure or node "
            "crash results in data loss. Additionally, search throughput "
            "cannot be distributed across replica shards.\n\n"
            "Zero replicas are acceptable during bulk loading, for indices "
            "backed by searchable snapshots (frozen tier), or for data that "
            "can be fully re-derived from an external source."
        ),
        "how": (
            "Set number_of_replicas to 1 (or more for high-availability):\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "number_of_replicas": 1 } }\n\n'
            "To set for all future indices, update the index template:\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "settings": { "number_of_replicas": 1 } } }\n\n'
            "This takes effect immediately on existing indices — Elasticsearch "
            "will start allocating replica shards."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/index-modules.html#dynamic-index-number-of-replicas"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 4
    # ------------------------------------------------------------------
    "replica_waste": {
        "category": "settings_audit",
        "severity": "info",
        "why": Template(
            "This index has $replicas replica(s) but is on the "
            "$tier_label tier. Indices on "
            "cold/frozen tiers are backed by searchable snapshots — the "
            "snapshot repository provides redundancy, making replicas "
            "unnecessary. Each replica doubles storage requirements and "
            "shard count without adding fault tolerance.\n\n"
            "Removing replicas on cold/frozen tiers is safe and saves "
            "significant disk space and cluster overhead."
        ),
        "how": (
            "Set replicas to 0:\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "number_of_replicas": 0 } }\n\n'
            "Elasticsearch will deallocate replica shards and free the "
            "storage immediately."
        ),
        "reference_url": (
            "https://www.elastic.co/docs/deploy-manage/tools/"
            "snapshot-and-restore/searchable-snapshots"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 5
    # ------------------------------------------------------------------
    "codec_opportunity": {
        "category": "settings_audit",
        "severity": "info",
        "why": Template(
            "This index uses the default LZ4 codec but is no longer "
            "receiving writes ($reason). Switching to best_compression "
            "(DEFLATE) typically reduces index size by 15-25%. Since the "
            "index is not being written to, the slower compression speed "
            "has no impact. In some cases, best_compression actually "
            "improves search performance because smaller data fits "
            "better in the filesystem cache.\n\n"
            "Note: Codec changes only apply to new segments. To apply "
            "to existing data, force-merge after changing the codec."
        ),
        "how": (
            "1. Update the codec setting:\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "codec": "best_compression" } }\n\n'
            "2. Force-merge to rewrite segments with the new codec:\n\n"
            "  POST /<index>/_forcemerge?max_num_segments=1\n\n"
            "For future indices, set the codec in the index template:\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "settings": { "codec": "best_compression" } } }'
        ),
        "reference_url": (
            "https://www.elastic.co/search-labs/blog/"
            "improve-elasticsearch-performance-best-compression"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 6
    # ------------------------------------------------------------------
    "field_count_near_limit": {
        "category": "settings_audit",
        "severity": "warning",
        "why": Template(
            "This index has $field_count mapped fields, which is $pct% of "
            "the total_fields.limit ($limit). If a new field is dynamically "
            "mapped and the limit is exceeded, indexing requests will fail "
            "with an error. High field counts also increase cluster state "
            "size, slow down mapping updates, and consume more heap on every "
            "node.\n\n"
            "Common causes: dynamic mapping with semi-structured data, "
            "flattening deeply nested JSON, or index templates that don't "
            "restrict field creation. A single index with 30,000+ fields "
            "can crash a cluster from mapping metadata overhead alone "
            "(documented in Elastic's 'Six Ways to Crash Elasticsearch')."
        ),
        "how": (
            "Option 1 — Switch to explicit mappings and disable dynamic:\n\n"
            "  PUT /<index>/_mapping\n"
            '  { "dynamic": "strict" }\n\n'
            "Option 2 — Use the flattened field type for variable-key data "
            "(labels, tags, user-defined metadata):\n\n"
            '  "metadata": { "type": "flattened" }\n\n'
            "Option 3 — Increase the limit (last resort):\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index.mapping.total_fields.limit": 2000 }\n\n'
            "Option 4 — Review unused fields with the Mapping Recommendations "
            "dashboard and remove fields nobody uses."
        ),
        "reference_url": (
            "https://www.elastic.co/docs/troubleshoot/elasticsearch/"
            "mapping-explosion"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 7
    # ------------------------------------------------------------------
    "source_disabled": {
        "category": "settings_audit",
        "severity": "critical",
        "why": (
            "This index has _source disabled. Without _source, Elasticsearch "
            "cannot reindex data, run update_by_query, use highlights, or "
            "access the original document in scripts. Crucially, Elasticsearch "
            "version upgrades that require reindexing will fail — this index "
            "becomes a dead end.\n\n"
            "Elastic strongly recommends against disabling _source. The "
            "storage savings are rarely worth the loss of functionality. "
            "Consider synthetic _source (ES 8.4+) as an alternative that "
            "saves storage while preserving reindex capability.\n\n"
            "If not addressed: this index cannot be migrated to future ES "
            "versions that require reindexing, and any data correction "
            "requiring update_by_query is impossible."
        ),
        "how": (
            "WARNING: Re-enabling _source requires creating a new index and "
            "re-ingesting data from the original source (not reindex, since "
            "_source is not available).\n\n"
            "1. Create a new index template with _source enabled (the default):\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "mappings": { "_source": { "enabled": true } } } }\n\n'
            "2. Re-ingest data from the original data pipeline.\n\n"
            "For new indices, consider synthetic _source as a middle ground "
            "(requires Enterprise license):\n\n"
            '  "_source": { "mode": "synthetic" }'
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/mapping-source-field.html"
        ),
        "breaking_change": True,
    },

    # ------------------------------------------------------------------
    # Rule 8
    # ------------------------------------------------------------------
    "rollover_lookback_mismatch": {
        "category": "usage_based",
        "severity": "warning",
        "why": Template(
            "Indices roll over approximately every $rollover_label, but "
            "95% of queries look back $lookback_label. This means each "
            "query must search across ~$indices_hit indices. Every "
            "additional index adds shard coordination overhead — the query "
            "coordinator must send requests to each shard, wait for all "
            "responses, and merge results.\n\n"
            "Reducing the number of indices per query improves search "
            "latency, reduces thread pool pressure, and simplifies cluster "
            "state. This is the single most impactful index architecture "
            "optimization for time-series data.\n\n"
            "This recommendation is based on actual query patterns observed "
            "through the gateway, not theoretical thresholds."
        ),
        "how": Template(
            "Option 1 — Increase the rollover time threshold so each index "
            "covers a longer period:\n\n"
            "  PUT _ilm/policy/<policy-name>\n"
            '  { "policy": { "phases": { "hot": { "actions": {\n'
            '    "rollover": { "max_age": "$lookback_label" }\n'
            "  } } } } }\n\n"
            "Option 2 — Switch to size-based rollover to decouple index "
            "lifespan from calendar time:\n\n"
            '  "rollover": { "max_primary_shard_size": "50gb" }\n\n'
            "Changes only affect new indices. Existing indices remain "
            "as-is until they age out via ILM."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/size-your-shards.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 9
    # ------------------------------------------------------------------
    "index_sorting_opportunity": {
        "category": "usage_based",
        "severity": "info",
        "why": Template(
            "$pct% of queries with sort clauses on this index group sort "
            "by '$dominant_field'. When an index is pre-sorted by this "
            "field, Elasticsearch can terminate searches early — it finds "
            "the top-N results without scanning every document. For sorted "
            "queries, this can reduce search time dramatically. Pre-sorting "
            "also improves compression because similar values are grouped "
            "together, reducing disk usage.\n\n"
            "Caveat: Index sorting slows write throughput by approximately "
            "40-50% because documents must be sorted at flush and merge "
            "time. This is best for indices with moderate write volume where "
            "search performance is the priority.\n\n"
            "This recommendation is based on actual sort patterns observed "
            "through the gateway."
        ),
        "how": Template(
            "Set index sorting in the index template (cannot be changed "
            "on existing indices):\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "settings": {\n'
            '    "index.sort.field": "$dominant_field",\n'
            '    "index.sort.order": "desc"\n'
            "  } } }\n\n"
            "New indices created from this template will be pre-sorted. "
            "The sort field must be a keyword, numeric, date, or boolean "
            "type with doc_values enabled.\n\n"
            "WARNING: This slows indexing by ~40-50%. Do not apply to "
            "write-heavy indices where ingest speed is critical."
        ),
        "reference_url": (
            "https://www.elastic.co/blog/index-sorting-elasticsearch-6-0"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 10
    # ------------------------------------------------------------------
    "refresh_interval_opportunity": {
        "category": "usage_based",
        "severity": "info",
        "why": Template(
            "This index group receives significantly more writes than "
            "searches ($write_count writes vs $search_count searches "
            "in the observation window). The default 1-second refresh "
            "interval creates a new Lucene segment every second, which "
            "increases indexing overhead and triggers frequent segment "
            "merges. Since few searches are running, the near-real-time "
            "freshness provided by 1s refresh is wasted.\n\n"
            "Increasing the refresh interval to 30s can improve indexing "
            "throughput by 20-30% and reduce segment merge pressure. "
            "Elasticsearch automatically skips refreshes for indices that "
            "haven't received a search in 30 seconds (search_idle), but "
            "explicitly setting a longer interval is more predictable.\n\n"
            "This recommendation is based on actual read/write ratios "
            "observed through the gateway."
        ),
        "how": (
            "Set a longer refresh interval:\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "refresh_interval": "30s" } }\n\n'
            "For write-heavy ingest with batch processing, consider "
            "disabling refresh entirely during bulk loads:\n\n"
            '  { "index": { "refresh_interval": "-1" } }\n\n'
            "Remember to restore a normal interval after bulk loading. "
            "For future indices, set this in the index template."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/tune-for-indexing-speed.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 11
    # ------------------------------------------------------------------
    "translog_async": {
        "category": "settings_audit",
        "severity": "warning",
        "why": (
            "This index has translog durability set to 'async'. In async mode, "
            "Elasticsearch only fsyncs the translog every 5 seconds (by default) "
            "instead of after every index/delete/bulk operation. If the node "
            "crashes or loses power, up to 5 seconds of acknowledged writes "
            "are permanently lost — the client received a success response but "
            "the data never made it to disk.\n\n"
            "Async translog is sometimes set during initial bulk loading for "
            "speed and then forgotten. It is appropriate ONLY when data loss is "
            "acceptable (e.g., metrics that can be re-derived, or during a "
            "one-time migration with retry logic).\n\n"
            "Unlike replica loss (which is visible as a yellow cluster), "
            "translog data loss is invisible — no error, no alert, just "
            "missing documents."
        ),
        "how": (
            "Set translog durability back to the default (request):\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "translog": { "durability": "request" } } }\n\n'
            "This takes effect immediately — no reindex or restart needed. "
            "Write throughput may decrease by 10-20% because each operation "
            "now waits for an fsync, but data safety is guaranteed.\n\n"
            "For future indices, remove any translog.durability override from "
            "the index template (the default is 'request')."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/index-modules-translog.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 12
    # ------------------------------------------------------------------
    "force_merge_opportunity": {
        "category": "settings_audit",
        "severity": "info",
        "why": Template(
            "This read-only index has approximately $avg_seg segments per "
            "primary shard. Since the index is no longer receiving writes, "
            "these segments will never be merged automatically. Each extra "
            "segment increases search overhead — Lucene must open file "
            "handles, maintain in-memory data structures, and merge results "
            "from each segment during every search.\n\n"
            "Force-merging a read-only index to 1 segment per shard "
            "reduces heap usage, file handle consumption, and search "
            "latency. It also improves compression because a single large "
            "segment compresses better than many small ones.\n\n"
            "IMPORTANT: Never force-merge a writable index — it produces "
            "very large segments that cannot be efficiently merged later."
        ),
        "how": (
            "Force-merge to 1 segment per shard:\n\n"
            "  POST /<index>/_forcemerge?max_num_segments=1\n\n"
            "This is a resource-intensive operation — run it during off-peak "
            "hours. Monitor progress with:\n\n"
            "  GET /_cat/tasks?v&actions=*forcemerge*\n\n"
            "For large indices, consider merging to a small number of "
            "segments first (e.g., max_num_segments=5) and then to 1 in a "
            "second pass to reduce peak disk usage during the merge.\n\n"
            "If the index also uses the default codec, combine force-merge "
            "with a codec change for maximum benefit:\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "codec": "best_compression" } }\n'
            "  POST /<index>/_forcemerge?max_num_segments=1"
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/indices-forcemerge.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 13
    # ------------------------------------------------------------------
    "node_shard_count": {
        "category": "cluster_health",
        "severity": "warning",
        "why": Template(
            "Node '$node' hosts $count shards. Elasticsearch "
            "recommends keeping shard count below 1,000 per node. Each "
            "shard consumes heap memory for segment metadata, Lucene "
            "instances, and thread pool resources — regardless of shard "
            "size. High shard counts cause:\n\n"
            "- Slow cluster state updates (every shard change is broadcast)\n"
            "- Increased GC pressure from accumulated segment metadata\n"
            "- Risk of circuit breaker trips during bulk operations\n"
            "- Longer recovery times after node restarts\n\n"
            "$impact_note"
        ),
        "how": (
            "Reduce shard count through one or more strategies:\n\n"
            "1. Delete old indices that are past retention:\n"
            "   DELETE /<old-index-pattern-*>\n\n"
            "2. Reduce replica count on cold/read-only indices:\n"
            "   PUT /<index>/_settings\n"
            '   { "index": { "number_of_replicas": 0 } }\n\n'
            "3. Merge small indices using the Shrink API:\n"
            "   POST /<index>/_shrink/<target>\n"
            '   { "settings": { "index.number_of_shards": 1 } }\n\n'
            "4. Use fewer shards per index in index templates:\n"
            "   PUT _index_template/<template>\n"
            '   { "template": { "settings": { "number_of_shards": 1 } } }\n\n'
            "5. Add more data nodes to distribute shards across more nodes.\n\n"
            "The per-node shard limit can be enforced with:\n"
            "   PUT _cluster/settings\n"
            '   { "persistent": { '
            '"cluster.max_shards_per_node": 1000 } }'
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/size-your-shards.html#shard-count-recommendation"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 14
    # ------------------------------------------------------------------
    "merge_policy_tuning": {
        "category": "settings_audit",
        "severity": "info",
        "why": Template(
            "Primary shards average $avg_str, but the merge policy's "
            "max_merged_segment is at the default 5GB. This means Lucene "
            "will never merge segments larger than 5GB into a single "
            "segment. For a 50GB+ shard, this results in at least 10 "
            "segments that can never be merged further — each consuming "
            "file handles, heap for metadata, and adding per-segment "
            "search overhead.\n\n"
            "Increasing max_merged_segment for large shards allows Lucene "
            "to produce fewer, larger segments. This reduces the segment "
            "count floor and improves search performance. The tradeoff is "
            "that merge operations take longer and use more temporary disk "
            "space.\n\n"
            "This is especially impactful for indices that are no longer "
            "receiving writes (combined with force merge)."
        ),
        "how": (
            "Increase the max merged segment size:\n\n"
            "  PUT /<index>/_settings\n"
            '  { "index": { "merge": { "policy": {\n'
            '    "max_merged_segment": "25gb"\n'
            "  } } } }\n\n"
            "Recommended value: approximately half the target shard size. "
            "For 50GB shards, use 25gb. For 100GB shards, use 50gb.\n\n"
            "After changing, a force merge will apply the new policy:\n\n"
            "  POST /<index>/_forcemerge?max_num_segments=1\n\n"
            "For future indices, set this in the index template."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/index-modules-merge.html"
        ),
        "breaking_change": False,
    },

    # ------------------------------------------------------------------
    # Rule 15
    # ------------------------------------------------------------------
    "shard_docs_limit": {
        "category": "shard_sizing",
        "severity": "warning",
        "why": Template(
            "A primary shard in this index group contains $docs_label "
            "documents, exceeding the recommended 200M soft limit. Lucene "
            "uses a signed 32-bit integer for internal document IDs, with "
            "a hard maximum of ~2.1 billion. However, performance degrades "
            "well before that limit:\n\n"
            "- Term dictionary lookups become slower with very large segments\n"
            "- Merges take significantly longer and consume more temporary "
            "disk space\n"
            "- Memory usage for segment metadata scales with document count\n"
            "- Recovery after node failure takes longer (more docs to replay)\n\n"
            "$impact_note"
        ),
        "how": Template(
            "Option 1 — Increase the shard count in the index template:\n\n"
            "  PUT _index_template/<template-name>\n"
            '  { "template": { "settings": { "number_of_shards": '
            "$shard_target } } }\n\n"
            "Option 2 — Enable ILM rollover with a document count threshold:\n\n"
            "  PUT _ilm/policy/<policy-name>\n"
            '  { "policy": { "phases": { "hot": { "actions": {\n'
            '    "rollover": { "max_docs": 200000000 }\n'
            "  } } } } }\n\n"
            "Option 3 — Use the Split API on existing indices:\n\n"
            "  POST /<index>/_split/<target-index>\n"
            '  { "settings": { "index.number_of_shards": '
            "$shard_target } }\n\n"
            "Note: The index must be read-only before splitting."
        ),
        "reference_url": (
            "https://www.elastic.co/guide/en/elasticsearch/reference/"
            "current/size-your-shards.html"
        ),
        "breaking_change": False,
    },
}
