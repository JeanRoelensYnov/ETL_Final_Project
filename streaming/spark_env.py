import os

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)

# /!\ EFFET DE BORD : `import spark_env` ne renvoie aucune valeur — il MODIFIE
# l'environnement du processus (os.environ) au moment de l'import. À importer
# AVANT pyspark : ces variables doivent exister avant que Spark ne démarre la JVM.
# (C'est aussi pourquoi l'import porte un `# noqa: F401` : il "sert" par son effet.)
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

_hadoop_home = os.path.join(_root, "tools", "hadoop")
os.environ["HADOOP_HOME"] = _hadoop_home
os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + os.pathsep + os.environ["PATH"]
