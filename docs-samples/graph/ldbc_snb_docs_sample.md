# LDBC_SNB

This is the social network dataset used in the documentation for graph in Microsoft Fabric.

It contains the following kinds of tables:

- `ldbc_snb_node_XXX`: Node table for `:XXX` nodes.
- `ldbc_snb_edge_FROM_XXX_TO`: Edge table for `:XXX` edges from `:FROM` to `:TO` nodes.

Every row in a given node table is uniquely identified by the value in the "id" column.

Edge tables link to `:XXX` source and `:YYY` destination nodes via their `src_XXX_id` and `dst_YYY_id` columns.
Edge tables do not contaiun duplicate rows.

All other columns contain properties.

Certain tables have additional, synthetically generated properties (`labels`, `super`) not covered by the associated graph type.  These can be safely ignored.
