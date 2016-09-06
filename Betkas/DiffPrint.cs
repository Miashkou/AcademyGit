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
            var similiarString = string.Empty;
            var originalSplit = original.Split(Row.columnValueSeperatorChar);

            foreach (var otherString in stringList)
            {
                var score = 0;
                var otherSplit = otherString.Split(Row.columnValueSeperatorChar);
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
            string[] thisList = original.Split(Row.columnValueSeperatorChar);
            string[] otherList = similarString.Split(Row.columnValueSeperatorChar);
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

                var diffSplit = diff.Split(Row.columnValueSeperatorChar);
                var bestGuessSplit = bestGuess.Split(Row.columnValueSeperatorChar);

                PrintWithDifferences(diffSplit, differences);
                PrintWithDifferences(bestGuessSplit, differences);
                Console.WriteLine();
            }
        }

        public static void PrintWithDifferences(string[] strings, List<int> diffIndexes, ConsoleColor color = ConsoleColor.DarkCyan)
        {
            for (int i = 0; i < strings.Length; i++)
            {
                if (diffIndexes.Contains(i))
                {
                    Console.BackgroundColor = color;
                    Console.Write(strings[i]);
                    Console.ResetColor();
                }
                else
                {
                    Console.Write(strings[i]);
                }
                Console.Write(Row.columnValueSeperatorStr);
            }
            Console.WriteLine();
        }
    }
}
