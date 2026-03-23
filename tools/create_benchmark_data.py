"""
Genere les fichiers .dat pour le benchmark naif vs optimise.

Tables creees :
  - data/commandes.dat   : 10000 tuples x 3 attributs (id, montant, produit_id)
  - data/produits.dat    :  1000 tuples x 2 attributs (id, prix)
  - data/ventes.dat      :  5000 tuples x 3 attributs (id, montant, client_id)
  - data/clients.dat     :  3000 tuples x 2 attributs (id, score)
  - data/stocks.dat      : 10000 tuples x 3 attributs (id, categorie, quantite)
  - data/factures.dat    :  5000 tuples x 3 attributs (id, total, fournisseur_id)
  - data/fournisseurs.dat:  1000 tuples x 2 attributs (id, note)
  - data/employes.dat    :  8000 tuples x 3 attributs (id, salaire, dept_id)
  - data/departements.dat:   200 tuples x 2 attributs (id, budget)
"""

import os
import struct
import random

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")


def write_table(filename, nb_tuples, nb_att, generator):
    """Ecrit une table binaire au format TableDisque."""
    path = os.path.join(DATA_DIR, filename)
    with open(path, "wb") as f:
        f.write(struct.pack("II", nb_tuples, nb_att))
        for i in range(nb_tuples):
            vals = generator(i)
            for v in vals:
                f.write(struct.pack("i", v))
    print(f"  {filename:.<30} {nb_tuples:>6} tuples x {nb_att} att")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    rng = random.Random(42)

    print("Generation des donnees de benchmark :")
    print()

    # COMMANDES : 10000 x 3 — FK produit_id dans [0, 1000)
    write_table("commandes.dat", 10000, 3, lambda i: [
        i, rng.randrange(10000), rng.randrange(1000),
    ])

    # PRODUITS : 1000 x 2
    write_table("produits.dat", 1000, 2, lambda i: [
        i, rng.randrange(5000),
    ])

    # VENTES : 5000 x 3 — FK client_id dans [0, 500)
    write_table("ventes.dat", 5000, 3, lambda i: [
        i, rng.randrange(10000), rng.randrange(500),
    ])

    # CLIENTS : 3000 x 2
    write_table("clients.dat", 3000, 2, lambda i: [
        i, rng.randrange(1000),
    ])

    # STOCKS : 10000 x 3 — categorie dans [0, 1000)
    write_table("stocks.dat", 10000, 3, lambda i: [
        i, rng.randrange(1000), rng.randrange(500),
    ])

    # FACTURES : 5000 x 3 — FK fournisseur_id dans [0, 1000)
    write_table("factures.dat", 5000, 3, lambda i: [
        i, rng.randrange(50000), rng.randrange(1000),
    ])

    # FOURNISSEURS : 1000 x 2
    write_table("fournisseurs.dat", 1000, 2, lambda i: [
        i, rng.randrange(100),
    ])

    # EMPLOYES : 8000 x 3 — FK dept_id dans [0, 200)
    write_table("employes.dat", 8000, 3, lambda i: [
        i, rng.randrange(100000), rng.randrange(200),
    ])

    # DEPARTEMENTS : 200 x 2
    write_table("departements.dat", 200, 2, lambda i: [
        i, rng.randrange(10000),
    ])

    print(f"\nFichiers ecrits dans {os.path.abspath(DATA_DIR)}/")


if __name__ == "__main__":
    main()
