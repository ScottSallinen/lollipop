


- A: Hashtable, aggregation, zero direct message passing
- B:
- C: No message aggregation, using array to store neighbours, destination of each edge is treated as a new neighbour
- D: No message aggregation, using array to store neighbours, no duplicate neighbours
- E: No message aggregation, using array to store neighbours, no duplicate neighbours, tracks incoming ResCap
- E: No message aggregation, using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0
- G: With message aggregation (copy), using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0
- H: With message aggregation (shared heights array), using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0