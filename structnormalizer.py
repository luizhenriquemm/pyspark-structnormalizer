import pyspark.sql.types as T
import pyspark.sql.functions as F
import builtins as b

def StructNormalizer(df):
  def SchemaIterator(schema, root=True, prefix=[]):
    f = []
    for c in schema:
      if c.dataType.typeName() == "struct":
        f += SchemaIterator(schema=c.dataType, root=False, prefix=[*prefix, c.name])
      elif not root:
        f.append([*prefix, c.name])
    return f
  nested_cols = SchemaIterator(df.schema)
  s = []
  for c in df.columns:
    if c not in [x[0] for x in nested_cols]:
      s.append(F.col(c))
    else:
      s += [F.col(".".join(x)).alias("_".join(x)) for x in b.filter(None, [x if x[0] == c else None for x in nested_cols])]
  return df.select(*s)
