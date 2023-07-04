


- A: Hashtable, aggregation, zero direct message passing
- B:
- C: No message aggregation, using array to store neighbours, destination of each edge is treated as a new neighbour
- D: No message aggregation, using array to store neighbours, no duplicate neighbours
- E: No message aggregation, using array to store neighbours, no duplicate neighbours, tracks incoming ResCap
- F: No message aggregation, using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0
- G: Message passing with aggregation (copy heights on retrieve), using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0
- H: Message passing with aggregation (directly access heights in the inbox), using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0
- I: Based on F, Pure message passing, using array to store neighbours, no duplicate neighbours, tracks incoming ResCap, skip sending heights if ResCapIn == 0, track index of the next push target in discharge