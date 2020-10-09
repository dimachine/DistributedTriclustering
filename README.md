# DistributedTriclustering


This is an archived version of a distributed triclustering algorithm implemented by Sergey Zudin under my supervision and Dmitry Gnatyshak guidance.

[Sergey Zudin, Dmitry V. Gnatyshak, Dmitry I. Ignatov:
Putting OAC-triclustering on MapReduce. CLA 2015: 47-58]
(http://ceur-ws.org/Vol-1466/paper04.pdf)

Abstract
In our previous work an efficient one-pass online algorithm for triclustering of binary data (triadic formal contexts) was proposed. This algorithm is a modified version of the basic algorithm for OAC-triclustering approach; it has linear time and memory complexities. In this paper we parallelise it via map-reduce framework in order to make it suitable for big datasets. The results of computer experiments show the efficiency of the proposed algorithm; for example, it outperforms the online counterpart on Bibsonomy dataset with ≈ 800, 000 triples.


It is based on the earlier work on a one-pass online algorithm for triclustering.


[Dmitry Gnatyshak, Dmitry I. Ignatov, Sergei O. Kuznetsov, Lhouari Nourine:
A One-pass Triclustering Approach: Is There any Room for Big Data? CLA 2014: 231-242]
(http://ceur-ws.org/Vol-1252/cla2014_submission_26.pdf)


References in BibTeX:

@inproceedings{Zudin:2015,
  author    = {Sergey Zudin and
               Dmitry V. Gnatyshak and
               Dmitry I. Ignatov},
  title     = {Putting OAC-triclustering on MapReduce},
  booktitle = {Proceedings of the Twelfth International Conference on Concept Lattices
               and Their Applications, Clermont-Ferrand, France, October 13-16, 2015},
  pages     = {47--58},
  year      = {2015},
  crossref  = {DBLP:conf/cla/2015},
  url       = {http://ceur-ws.org/Vol-1466/paper04.pdf}
}

@proceedings{DBLP:conf/cla/2015,
  editor    = {Sadok Ben Yahia and
               Jan Konecny},
  title     = {Proceedings of the Twelfth International Conference on Concept Lattices
               and Their Applications, Clermont-Ferrand, France, October 13-16, 2015},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {1466},
  publisher = {CEUR-WS.org},
  year      = {2015}
}

@inproceedings{Gnatyshak:2014,
  author    = {Dmitry Gnatyshak and
               Dmitry I. Ignatov and
               Sergei O. Kuznetsov and
               Lhouari Nourine},
  title     = {A One-pass Triclustering Approach: Is There any Room for Big Data?},
  booktitle = {Proceedings of the Eleventh International Conference on Concept Lattices
               and Their Applications, Ko{\v{s}}ice, Slovakia, October 7-10, 2014},
  pages     = {231--242},
  year      = {2014},
  crossref  = {DBLP:conf/cla/2014},
  url       = {http://ceur-ws.org/Vol-1252/cla2014\_submission\_26.pdf},
  urn       = {urn:nbn:de:0074-1466-5}
}

@proceedings{DBLP:conf/cla/2014,
  editor    = {Karell Bertet and
               Sebastian Rudolph},
  title     = {Proceedings of the Eleventh International Conference on Concept Lattices
               and Their Applications, Ko{\v{s}}ice, Slovakia, October 7-10, 2014},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {1252},
  publisher = {CEUR-WS.org},
  year      = {2014},
  url       = {http://ceur-ws.org/Vol-1252},
  urn       = {urn:nbn:de:0074-1252-0}
}




