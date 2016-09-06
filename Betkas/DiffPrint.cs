using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Comperator
{
    class DiffPrint
    {
        public static string GetSimiliarString(string original, IEnumerable<string> stringList)
        {
            var bestScore = 0;
            var similiarString = String.Empty;
            var originalSplit = original.Split('|');

            foreach (var otherString in stringList)
            {
                var score = 0;
                var otherSplit = otherString.Split('|');
                for (int i = 0; i < originalSplit.Length; i++)
                {
                    if (originalSplit[i] == otherSplit[i])
                    {
                        score++;
                    }
                }

                if (score > bestScore)
                {
                    similiarString = otherString;
                    bestScore = score;
                }
            }

            return similiarString;
        }

        public static List<int> FindDifferenceIndexes(string similarString, string original)
        {
            string[] thisList = original.Split('|');
            string[] otherList = similarString.Split('|');
            List<int> differences = new List<int>();
            for (int i = 0; i < thisList.Length; i++)
            {
                if (thisList[i] != otherList[i])
                    differences.Add(i);
            }
            return differences;
        }

        public static void Coloring(List<string> list1, List<string> list2)
        {
            foreach (var diff in list1)
            {
                var bestGuess = GetSimiliarString(diff, list2);

                var differences = FindDifferenceIndexes(diff, bestGuess);

                var diffSplit = diff.Split('|');
                var bestGuessSplit = bestGuess.Split('|');

                PrintWithDifferences(diffSplit, differences);
                PrintWithDifferences(bestGuessSplit, differences);
                Console.WriteLine();
            }
        }

        public static void PrintWithDifferences(string[] strings, List<int> diffIndexes, ConsoleColor color = ConsoleColor.DarkCyan)
        {
            string[] tableAtributes = Query.ListOfAtributesName.ToArray();

            for (int i = 0; i < strings.Length; i++)
            {

                if (diffIndexes.Contains(i))
                {
                    Console.BackgroundColor = color;
                    Console.Write(tableAtributes[i] + " - " + strings[i]);
                    Console.ResetColor();
                    Console.Write("|");
                }
                else
                {

                    if (i < 4)
                    {
                        Console.Write(tableAtributes[i] + " - " + strings[i]);
                        Console.Write("|");
                    }

                }
            }
            Console.WriteLine();
        }
    }
}
