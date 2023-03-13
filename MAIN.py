import Licceu
import PMO
import MERGE
import timeit
inicio = timeit.default_timer()

PMO.processArchive()
Licceu.processArchive()


MERGE.processArchive()




fim = timeit.default_timer()
print('duracao: %f' % ((fim - inicio)/60) + ' min')