# 0000 — Combine bmw-etl pipeline with carparts-etl schema

Status: accepted

Context: I had two half-working repos. bmw-etl has the more sophisticated pipeline
skeleton; carparts-etl has a database with defined schemas. The BMW parts market has
many vendors per part (specialists, dealer networks, warehouse retailers) selling
parts made by many brands, so prices vary by vendor and over time. I want one
portfolio project.

Options:
  A. bmw-etl only — one price per part, overwritten; history is lost. Poor dimensional modelling.
  B. carparts-etl only — the consumer only logs messages; nothing reaches Postgres.
  C. Combine: bmw-etl pipeline + carparts-etl schema.

Decision: C, because the prices table lets me see how part prices fluctuate, per
vendor and over time, and relate them to external drivers (e.g. the rand exchange rate).

Consequences:
  Easier: price history per part and per vendor.
  Harder: the part_id and vendor_id foreign keys must resolve, so parent rows must
          exist before a price is inserted.
  Revisit if: the data source only ever provides one vendor per part.

Follow-up: carparts-etl made private.
