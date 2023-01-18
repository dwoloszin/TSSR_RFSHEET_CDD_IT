import Licceu
import PMO
import MERGE
import R041
import timeit
inicio = timeit.default_timer()


#R041.processArchive()
#Licceu.processArchive()


PMO.processArchive()
Licceu.processArchive()


MERGE.processArchive()




fim = timeit.default_timer()
print('duracao: %f' % ((fim - inicio)/60) + ' min')