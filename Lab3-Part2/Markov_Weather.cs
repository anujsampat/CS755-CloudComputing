using Meta.Numerics;
using Meta.Numerics.Matrices;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            SquareMatrix T = new SquareMatrix(3);
            
            // Construct the transition matrix for the weather.
            T[0,0] = 0.5; // sunny to sunny
            T[1, 0] = 0.4; // sunny to rainy
            T[2, 0] = 0.1; // sunny to cloudy

            T[0,1] = 0.3; // rainy to sunny
            T[1, 1] = 0.4; // rainy to rainy
            T[2, 1] = 0.3; // rainy to cloudy

            T[0, 2] = 0.2; // cloudy to sunny
            T[1, 2] = 0.3; // cloudy to rainy
            T[2, 2] = 0.5; // cloudy to cloudy
                        
            // verify Markov conditions
            for (int c = 0; c < T.Dimension; c++)
            {
                double sr = 0.0;
                for (int r = 0; r < T.Dimension; r++)
                {
                    sr += T[r, c];
                }
                Debug.Assert(Math.Abs(sr - 1.0) < 1.0e-15);
            }

            // Begin with some initial state vector, and repeatedly 
            // apply P until the resultant vector no longer changes. 
            // The resultant vector will be the dominant eigenvector.
            ComplexEigensystem E = T.Eigensystem();

            Complex e = -1.0;
            IList<Complex> v = null;
            Complex maxEI = E.Eigenvalue(0);
            v = E.Eigenvector(0);

            // Find the dominant eigenvector
            for (int i = 1; i < E.Dimension; i++)
            {
                Complex ei = E.Eigenvalue(i);
                if (ComplexMath.Abs(ei) > ComplexMath.Abs(maxEI))
                {
                    maxEI = ei;
                    v = E.Eigenvector(i);
                }
            }

            // verify that it has eigenvalue 1 and is real
            Debug.Assert(Math.Abs(maxEI.Re - 1.0) < 1.0e-15);
            Debug.Assert(maxEI.Im == 0.0);
            for (int i = 0; i < 3; i++)
            {
                Debug.Assert(v[i].Im == 0.0);
            }

            // normalize the probabilities
            double sv = 0.0;
            for (int i = 0; i < E.Dimension; i++)
            {
                sv += v[i].Re;
            }
            double[] p = new double[E.Dimension];
            for (int i = 0; i < E.Dimension; i++)
            {
                p[i] = v[i].Re / sv;
            }

            // print the probabilities (steady state of the weather)
            Console.Write("Sunny = ");
            Console.WriteLine(p[0]);
            Console.Write("Rainy = ");
            Console.WriteLine(p[1]);
            Console.Write("Cloudy = ");
            Console.WriteLine(p[2]);
            Console.ReadLine();
        }
    }
}