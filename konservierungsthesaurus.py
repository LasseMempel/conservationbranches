import requests
import pandas as pd
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import SKOS, RDF, DC, DCTERMS, RDFS, VANN
import json

def csv2Df(link, propertyMatchDict):
    with open("data.csv", "w", encoding="utf-8") as f:
        f.write(requests.get(link).text.encode("ISO-8859-1").decode())
    df = pd.read_csv('data.csv', encoding="utf-8")
    df.rename(columns=propertyMatchDict, inplace=True) # rename columns according to propertyMatchDict
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x) # remove leading and trailing whitespaces from all cells
    # fix to replace linebreaks with pipeseperators for mapping properties, which don't follow the seperator rules
    for col in ["closeMatch", "relatedMatch", "exactMatch"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: "|".join(x.split("\n")) if isinstance(x, str) else x)
    # delete columns hierarcylevel, type, creator
    df = df.drop(columns=["hierarcylevel", "type", "creator"])
    # add columns broadMatch and narrowMatch
    df["broadMatch"] = ""
    df["narrowMatch"] = ""
    df["narrower"] = ""


    return df

def integrateTranslationInLabels(df):
    # integrate translation column into prefLabel and altLabel
    for index, row in df.iterrows():
        if row["prefLabel"] and isinstance(row["prefLabel"], str) and row["notation"] and isinstance(row["notation"], str):
            # add @baseLangLabel to all prefLabels
            prefLabels = row["prefLabel"].split("|")
            if len(prefLabels) == 0:
                prefLabels = [row["prefLabel"]]
            prefLabels = [x.strip()+"@"+baseLanguageLabel for x in prefLabels]
            # join changed prefLabels with seperator "|" at row prefLabel
            df.at[index, "prefLabel"] = "|".join(prefLabels)

            if row["altLabel"] and isinstance(row["altLabel"], str):
                # add @baseLangLabel to all altLabels
                altLabels = row["altLabel"].split("|")
                if len(altLabels) == 0:
                    altLabels = [row["altLabel"]]
                altLabels = [x.strip()+"@"+baseLanguageLabel for x in altLabels]
                # join changed altLabels with seperator "|" at row altLabel
                df.at[index, "altLabel"] = "|".join(altLabels)

            # integrate translation column into pref and alt labels
            if row["translation"] and isinstance(row["translation"], str):

                labels = row["translation"].split("|")
                labels = [x.strip() for x in labels]
                langDict = {}
                for label in labels:
                    try:
                        term, lang = label.split("@")
                    except:
                        print(label)
                        raise Exception
                    if lang in langDict:
                        langDict[lang].append(term)
                    else:
                        langDict[lang] = [term]
                for language in langDict:
                    for i in range(len(langDict[language])):
                        if i == 0:
                            # add term to prefLabel
                            df.at[index, "prefLabel"] += "|" + langDict[language][i] + "@"+ language
                        else:
                            # add term to altLabel
                            if not isinstance(row["altLabel"], float):
                                df.at[index, "altLabel"] += "|" + langDict[language][i] + "@"+ language
                            else:
                                df.at[index, "altLabel"] = langDict[language][i] + "@"+ language
    # write data as csv
    #df.to_csv('integratedTranslation.csv', index=False)
    df = df.drop(columns=["translation"])
    return df

def useSemanticAatUris(df):
    for index, row in df.iterrows():
        if row["prefLabel"] and isinstance(row["prefLabel"], str) and row["notation"] and isinstance(row["notation"], str):
            # in columns "closeMatch" and "relatedMatch" replace "vocab.getty.edu/page/aat/" with vocab.getty.edu/aat/
            oldRelatedMatch = row["relatedMatch"]
            oldCloseMatch = row["closeMatch"]
            if oldRelatedMatch and isinstance(oldRelatedMatch, str):
                df.at[index, "relatedMatch"] = oldRelatedMatch.replace("vocab.getty.edu/page/aat/", "vocab.getty.edu/aat/")
            if oldCloseMatch and isinstance(oldCloseMatch, str):
                df.at[index, "closeMatch"] = oldCloseMatch.replace("vocab.getty.edu/page/aat/", "vocab.getty.edu/aat/")
    return df

def recursiveNotationGeneration(df, rootNotation):
    # create list of column notation where broader = notation
    notationList = [rootNotation]
    childrenNotations = df[df["broader"] == rootNotation]["notation"].tolist()
    for childNotation in childrenNotations:
        notationList.extend(recursiveNotationGeneration(df, childNotation))
    return notationList

def createPartitionTables(df, startingPoints):
    if startingPoints:
        # create df with rows where column notation is in array startingPoints
        partitionDf = df[df["notation"].isin(startingPoints)]
    else:
        # create df with rows where column broader has value "top"
        partitionDf = df[df["broader"] == "top"]

    print(partitionDf)

    # create new df with columns Zweig, URI, description
    schemeDf = pd.DataFrame(columns=["Name", "URI", "description", "creator", "publisher", "license", "rights", "contributor", "subjects", "hasTopConcept"])

    creator = "Kristina Fischer"
    publisher = "Leibniz-Zentrum für Archäologie (LEIZA)"
    license = "https://creativecommons.org/licenses/by/4.0/"
    rights = "CC BY 4.0"
    contributors = "|".join(["Kristina Fella", 
                    "Lasse Mempel-Länger", 
                    "Waldemar Muskalla", 
                    "Dr. Ingrid Stelzner", 
                    "Matthias Heinzel",
                    "Christian Eckmann",
                    "Heidrun Hochgesand",
                    "Katja Broschat",
                    "Leslie Pluntke",
                    "Markus Wittköpper",
                    "Marlene Schmucker",
                    "Prof. Dr. Roland Schwab",
                    "Rüdiger Lehnert",
                    "Ulrike Lehnert",
                    "Stephan Patscher",
                    "Lena Klar"
                    ])
    subjects = "|".join(["Konservierung", "Restaurierung", "Archäologie"])

    branchConceptNotationDict = {}
    narrowerDict = {}
    for index, row in partitionDf.iterrows():
        branchNotation = row["notation"]
        branchNotations = recursiveNotationGeneration(df, branchNotation)
        for notation in branchNotations:
            branchConceptNotationDict[notation] = branchNotation.strip()
            # find row in df with column "notation" = nptation and its "broader" value
            broader = df[df["notation"] == notation]["broader"].values[0]
            if isinstance(broader, str):
                if broader in narrowerDict:
                    narrowerDict[broader].append(notation)
                else:
                    narrowerDict[broader] = [notation]
    with open('branchConceptNotationDict.json', 'w', encoding="utf-8") as f:
        json.dump(branchConceptNotationDict, f, ensure_ascii=False, indent=4)
    with open('narrowerDict.json', 'w', encoding="utf-8") as f:
        json.dump(narrowerDict, f, ensure_ascii=False, indent=4)
    switchPropertyDict= {
        "broader": "broadMatch",
        "narrower": "narrowMatch",
        "related": "relatedMatch"
    }
    for index, row in partitionDf.iterrows():
        notation = row["notation"]
        prefLabel = row["prefLabel"].split("|")[0].split("@de")[0].strip("[]")
        print(notation, prefLabel)
        schemeURI = f"{branchUri}{notation}"
        schemeDf = pd.concat([schemeDf, pd.DataFrame({"Name": [prefLabel], "URI": [schemeURI], "description": [f" Zweig für {prefLabel} im Konservierungs- und Restaurierungsfachthesaurus für archäologische Kulturgüter (https://www.w3id.org/archlink/terms/conservationthesaurus)"], "creator": [creator], "publisher": [publisher], "license": [license], "rights": [rights], "contributor": [contributors], "subjects": [subjects], "hasTopConcept": [f"{baseUri}/{notation}"]})], ignore_index=True)
        branchNotations = recursiveNotationGeneration(df, notation)
        branchDf = df[df["notation"].isin(branchNotations)]
        for branchIndex, branchRow in branchDf.iterrows():
            branchDf.at[branchIndex, "uri"] = f"{baseUri}/{branchRow['notation']}"
            if branchRow["notation"] in narrowerDict:
                branchDf.at[branchIndex, "narrower"] = "|".join(f"{baseUri}/{x}" for x in narrowerDict[branchRow['notation']])
            switchProperties = ["broader", "related"]
            for switchProperty in switchProperties:
                if branchRow[switchProperty] and isinstance(branchRow[switchProperty], str):
                    switchNotations = branchRow[switchProperty].split("|")
                    switchNotations = [x.strip() for x in switchNotations]
                    switchUris = []
                    switchedUris = []
                    branchDf.at[branchIndex, switchProperty] = None
                    for switchNotation in switchNotations:
                        if switchNotation not in branchConceptNotationDict:
                            switchedUris.append(f"{baseUri}/{switchNotation}")
                        else:
                            if branchConceptNotationDict[switchNotation] ==  notation:
                                switchUris.append(f"{baseUri}/{switchNotation}")
                            else:
                                switchedUris.append(f"{branchUri+branchConceptNotationDict[switchNotation]}/{switchNotation}")
                    if pd.notna(branchDf.at[branchIndex, switchProperty]) and isinstance(branchDf.at[branchIndex, switchProperty], str):
                        existing_value = branchDf.at[branchIndex, switchProperty]
                        branchDf.at[branchIndex, switchProperty] = existing_value + "|" + "|".join(switchUris)
                    else:
                        branchDf.at[branchIndex, switchProperty] = "|".join(switchUris)
                    if pd.notna(branchDf.at[branchIndex, switchPropertyDict[switchProperty]]) and isinstance(branchDf.at[branchIndex, switchPropertyDict[switchProperty]], str):
                        existing_value = branchDf.at[branchIndex, switchPropertyDict[switchProperty]]
                        branchDf.at[branchIndex, switchPropertyDict[switchProperty]] = existing_value + "|" + "|".join(switchedUris)
                    else:
                        branchDf.at[branchIndex, switchPropertyDict[switchProperty]] = "|".join(switchedUris)
        branchDf.to_csv(f"{notation}.csv", index=False)
    schemeDf.to_csv("schemes.csv", index=False)

def row2Triple(i, g, concept, pred, obj, isLang, baseLanguageLabel, thesaurusAddendum, thesaurus):
    i = i.strip()
    if i == "":
        print("Empty cell")
        print(concept, pred, obj)
        return g
    if obj == URIRef:
        if pred in [SKOS.broader, SKOS.narrower, SKOS.related]:
            if i != "top":
                g.add ((concept, pred, URIRef(i)))
                #if pred == SKOS.broader:
                #    g.add ((URIRef(i), SKOS.narrower, concept)) 
            else:
                g.add ((concept, SKOS.topConceptOf, thesaurus))
        else:
            g.add ((concept, pred, URIRef(i))) #urllib.parse.quote(i)
    else:
        if isLang:
            if len(i) > 2 and i[-3] == "@":
                try:
                    i, baseLanguageLabel = i.split("@")
                except:
                    print(i)
                    raise Exception
            g.add ((concept, pred, obj(i, lang= baseLanguageLabel)))
        else:
            g.add ((concept, pred, obj(i)))
    return g

def df2Skos(ZweigDf, schemeURI, title, topConcept):
    propertyTuples = [
        ("notation", SKOS.notation, Literal, False),
        ("prefLabel", SKOS.prefLabel, Literal, True),
        ("altLabel", SKOS.altLabel, Literal, True),
        ("definition", SKOS.definition, Literal, True),
        ("broader", SKOS.broader, URIRef, False),
        ("narrower", SKOS.narrower, URIRef, False),
        ("related", SKOS.related, URIRef, False),
        ("closeMatch", SKOS.closeMatch, URIRef, False),
        ("relatedMatch", SKOS.relatedMatch, URIRef, False),
        ("exactMatch", SKOS.exactMatch, URIRef, False),
        ("broadMatch", SKOS.broadMatch, URIRef, False),
        ("narrowMatch", SKOS.narrowMatch, URIRef, False)
    ]

    extendedTuples = [
        ("source", SKOS.note, Literal, True), #DC.source # False
        #("creator", DC.creator, Literal, False),
        ("seeAlso", RDFS.seeAlso, Literal, False),
        ("translation", SKOS.altLabel, Literal, True)
    ]

    g = Graph()
    thesaurus = URIRef(schemeURI)
    thesaurusAddendum = URIRef(baseUri) + "/"

    g.add ((thesaurus, RDF.type, SKOS.ConceptScheme))
    g.add ((thesaurus, DC.title, Literal(title, lang=baseLanguageLabel)))
    g.add ((thesaurus, DC.description, Literal(f" Zweig für {title} im Konservierungs- und Restaurierungsfachthesaurus für archäologische Kulturgüter (https://www.w3id.org/archlink/terms/conservationthesaurus)", lang=baseLanguageLabel)))
    g.add ((thesaurus, DC.creator, Literal("Kristina Fischer")))
    g.add ((thesaurus, DCTERMS.publisher, Literal("Leibniz-Zentrum für Archäologie (LEIZA)")))
    g.add ((thesaurus, DCTERMS.license, URIRef("https://creativecommons.org/licenses/by/4.0/")))
    g.add ((thesaurus, DCTERMS.rights, Literal("CC BY 4.0")))
    g.add((thesaurus, VANN.preferredNamespaceUri, Literal(thesaurus+"/")))

    contributors = ["Kristina Fella", 
                    "Lasse Mempel-Länger", 
                    "Waldemar Muskalla", 
                    "Dr. Ingrid Stelzner", 
                    "Matthias Heinzel",
                    "Christian Eckmann",
                    "Heidrun Hochgesand",
                    "Katja Broschat",
                    "Leslie Pluntke",
                    "Markus Wittköpper",
                    "Marlene Schmucker",
                    "Dr. Roland Schwab",
                    "Rüdiger Lehnert",
                    "Ulrike Lehnert",
                    "Stephan Patscher",
                    "Lena Klar"
                    ]
    for contributor in contributors:
        g.add ((thesaurus, DCTERMS.contributor, Literal(contributor)))

    subjects = ["Konservierung", "Restaurierung", "Archäologie"]

    for subject in subjects:
        g.add ((thesaurus, DCTERMS.subject, Literal(subject, lang=baseLanguageLabel)))

    for index, row in ZweigDf.iterrows():
        if row["prefLabel"] and isinstance(row["prefLabel"], str) and row["notation"] and isinstance(row["notation"], str):
            #print(row["prefLabel"], row["notation"])
            concept = URIRef(thesaurusAddendum + row['notation'])
            g.add ((concept, RDF.type, SKOS.Concept))
            for prop, pred, obj, isLang in propertyTuples+extendedTuples:
                if prop in ZweigDf.columns:
                    if not isinstance(row[prop], float):
                        if seperator in row[prop]:
                            seperated = row[prop].split(seperator)
                            langs = [x.split("@") for x in seperated]
                            for i in range(len(seperated)):
                                g = row2Triple(seperated[i], g, concept, pred, obj, isLang, baseLanguageLabel, thesaurusAddendum, thesaurus)
                        else:
                            g = row2Triple(row[prop], g, concept, pred, obj, isLang, baseLanguageLabel, thesaurusAddendum, thesaurus)
            g.add ((concept, SKOS.inScheme, thesaurus))
    topConcept = URIRef(topConcept)
    g.add ((thesaurus, SKOS.hasTopConcept, topConcept))
    g.add ((topConcept, SKOS.topConceptOf, thesaurus))

    return g

def main():
    """
    df = csv2Df(link, propertyMatchDict)
    df = integrateTranslationInLabels(df)
    df = useSemanticAatUris(df)
    text = df.to_csv(index=False)
    with open('polishedData.csv', 'w', encoding="utf-8") as f:
        f.write(text)
    """
    df = pd.read_csv('polishedData.csv', encoding="utf-8")
    createPartitionTables(df, startingPoints)
    schemeDf = pd.read_csv("schemes.csv")
    for index, row in schemeDf.iterrows():
        schemeURI, title, topConcept = row["URI"], row["Name"], row["hasTopConcept"]
        notation = schemeURI.split("/")[-1]
        print("Working on: " + title)
        ZweigDf = pd.read_csv(f"{notation}.csv")
        graph = df2Skos(ZweigDf, schemeURI, title, topConcept)
        graph.serialize(destination=f'{notation}.ttl', format='turtle')   
        #graph.serialize(destination='thesaurus.json-ld', format='json-ld')
    

link =  "https://docs.google.com/spreadsheets/d/e/2PACX-1vSJV7qC1QYCAYghp8SX09EatvnXPurJ9ZMAsGE1iUrPIxL4nLiyXlYBtKBi1Zf1xTG10AXzUp3pZcxx/pub?gid=0&single=true&output=csv" # "https://docs.google.com/spreadsheets/d/e/2PACX-1vQCho2k88nLWrNSXj4Mgj_MwER5GQ9zbZ0OsO3X_QPa9s-3UkoeLLQHuNHoFMKqCFjWMMprKVHMZzOj/pub?gid=0&single=true&output=csv"
baseLanguageLabel = "de"
baseUri = "https://www.w3id.org/archlink/terms/conservationthesaurus" # "https://www.lassemempel.github.io/partitionedThesauri/" # "https://lassemempel.github.io/LEIZA-Terminologien/conservationthesaurus"   # "http://data.archaeology.link/terminology/archeologicalconservation"
branchUri = "https://www.w3id.org/archlink/branches/"

# dictionary to map divergent column names in the csv to the SKOS properties
propertyMatchDict = {"identifier":"notation","description":"definition","parent":"broader"}
seperator = "|"

# startingPoints for branches
startingPoints = [
"B51DAF", # Material
"F37GBB", # Behandlungsmethode
"C364G1", # Konservierungs- und Restaurierungsmaterialien
"C93638", # Konservierungs- und Restaurierungswerkzeuge
"D6BCC2", # Technologischer Befund
"DAC996", # Schadensphänomen
"GDF8C2", # Physischer Objektzustand
"F964CG", # Schadensursache
]

main()