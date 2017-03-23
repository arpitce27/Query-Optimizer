using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;

/**
  *    @auther Arpit Patel, Donil Rathod
  *    @Date 13th May 2016
  *    @Description : This program eveluates a cost of input query written in Input.txt
  *    
**/

namespace QueryOptimizer
{
    class Program
    {
        Disk_info DI = new Disk_info();
        static void Main(string[] args)
        {
            string path = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName;
            string filepath = path + @"\Input.txt";
            opt_choose_best_plan(filepath);
        }

        //This method is to convert disk I/O to hours
        private static String cost_as_time(double cost)
        {
            Disk_info DI = new Disk_info();
            double ms;
            Double hours;
            Double minutes;
            Double seconds;

            ms = cost * (DI.Seek_Time_in_ms + DI.Latency_in_ms);
            seconds = Math.Floor(ms / 1000);
            hours = Math.Floor(seconds / 3600);
            minutes = Math.Floor((seconds / 60) - (hours * 60));
            seconds = seconds - (minutes * 60) - (hours * 3600);

            return  "  " + Convert.ToInt32(hours) + " hours " + Convert.ToInt32(minutes) + " minutes " + Convert.ToInt32(seconds) + " seconds";

        }

        //This method is to choose best plan for query
        private static void opt_choose_best_plan(String path)
        {
            Table t1 = new Table("t1",20,10,false);
            Table t2 = new Table("t2", 40, 5, false);
            Table t3 = new Table("t3", 100, 20, false);
            Disk_info DI = new Disk_info();
            StreamReader sr = new StreamReader(path);

            Console.WriteLine("Input.txt file");
            string line;
            while ((line = sr.ReadLine()) != null)
            { 
                Console.WriteLine(line);
            }
            Console.WriteLine("-----------------------------------------");

            double Q1_total_cost = 0,Q1_projection_cost = 0,Q1_groupby_cost=0;
            double Q2_total_cost = 0, Q2_projection_cost = 0, Q2_groupby_cost = 0;
            string Q1_costin_hour, Q2_costin_hour;

            // Executing the cost of Query 1 (Q1)
            // Step 1 : temp1 <= t1 join t2
            ArrayList joinlist = new ArrayList();
            joinlist.Add("TNL");
            joinlist.Add("PNL");
            joinlist.Add("BNL");
            joinlist.Add("Hash_join");
            joinlist.Add("SMJ");
            Join Q1_temp1 = Join_cost(t1,t2,joinlist,"0.10","Q1 temp1 = t1 join t2");
            Table Q1temp1 = new Table("Q1temp1",Q1_temp1.Tuple_Size,Q1_temp1.Tuple_Count/(DI.Page_Size_in_Bytes/Q1_temp1.Tuple_Size),false);

            //Step2 : temp2 <= temp1 join t3
            ArrayList Q1temp2joinlist = new ArrayList();
            Q1temp2joinlist.Add("TNL");
            Q1temp2joinlist.Add("PNL");
            Join Q1_temp2 = Join_cost(Q1temp1, t3, Q1temp2joinlist, "0.20", "Q1 temp2 = temp1 join t3");
            Table Q1temp2 = new Table("Q1temp2", Q1_temp2.Tuple_Size, Q1_temp2.Tuple_Count / (DI.Page_Size_in_Bytes / Q1_temp2.Tuple_Size), false);

            //step3 : temp3 <= project temp2
            Q1_projection_cost = Projection(Q1temp2,false);

            //step4 : temp4 <= groupby temp3
            Q1_groupby_cost = GroupBy(Q1temp2,false);

            Q1_total_cost = Q1_temp1.Cost + Q1_temp2.Cost + Q1_projection_cost + Q1_groupby_cost;
            Q1_costin_hour = cost_as_time(Q1_total_cost);

            //-----------------------------------------------------------------------------------------------------------------------------------------
            // Executing the cost of Query 2 (Q2)  Console.WriteLine("Join cost  ");
            // Step 1 : temp1 <= t1 join t3
            Join Q2_temp1 = Join_cost(t1, t3, joinlist, "0.01", "Q2 temp1 = t1 join t3");
            Table Q2temp1 = new Table("Q2temp1", Q2_temp1.Tuple_Size, Q2_temp1.Tuple_Count / (DI.Page_Size_in_Bytes / Q2_temp1.Tuple_Size), false);
            //Console.WriteLine("Join cost Q2temp1 " + Q2_temp1.Cost);

            // Step 2 : temp2 <= t1 join temp1
            Join Q2_temp2 = Join_cost(t1, Q2temp1, joinlist, "0.15", "Q2 temp2 = t1 join temp1");
            Table Q2temp2 = new Table("Q2temp2", Q2_temp2.Tuple_Size, Q2_temp2.Tuple_Count / (DI.Page_Size_in_Bytes / Q2_temp2.Tuple_Size), false);

            // Step 3 : temp3 <= t2 join temp2
            Join Q2_temp3 = Join_cost(t2, Q2temp2, joinlist, "0.10", "Q2 temp3 = t2 join temp2");
            Table Q2temp3 = new Table("Q2temp3", Q2_temp3.Tuple_Size, Q2_temp3.Tuple_Count / (DI.Page_Size_in_Bytes / Q2_temp3.Tuple_Size), false);

            //step 4 : temp4 <= projection temp3
            Q2_projection_cost = Projection(Q2temp3, false);
            
            //step 5 : temp5 <= groupby temp4
            Q2_groupby_cost = GroupBy(Q2temp3, false);
           
            //Total cost of query 2
            Q2_total_cost = Q2_temp1.Cost + Q2_temp2.Cost + Q2_temp3.Cost + Q2_projection_cost + Q2_groupby_cost;
            Q2_costin_hour = cost_as_time(Q2_total_cost);
            

            Console.WriteLine();
            Console.WriteLine("---------------Q--------------");
            Console.WriteLine("               "+"Join Method used"+"  ");
            Console.WriteLine("Join t1 t2         "+Q1_temp1.Type);
            Console.WriteLine("Join temp1 t3      "+Q1_temp2.Type);
            Console.WriteLine("Project temp2  "+"");
            Console.WriteLine("Groupby temp3  "+"");
            Console.WriteLine("Total DISK I/O "+Q2_total_cost);
            Console.WriteLine("Processing Time"+Q2_costin_hour);
            Console.WriteLine();
            Console.WriteLine("--------------RQ--------------");
            Console.WriteLine("               " + "Join Method used" + "  ");
            Console.WriteLine("Join t1 t3          "+Q2_temp1.Type);
            Console.WriteLine("Join t1 temp1       "+Q2_temp2.Type);
            Console.WriteLine("Join t2 temp2       "+Q2_temp3.Type);
            Console.WriteLine("Project temp3  "+"");
            Console.WriteLine("Group temp4    "+"");
            Console.WriteLine("Total DISK I/O " + Q1_total_cost);
            Console.WriteLine("Processing Time" + Q1_costin_hour);
            Console.WriteLine();

        }

        //This method returns the join type from and all related join information like its which kind of join
        private static Join Join_cost (Table LTable, Table RTable, ArrayList Join_list, String selectivity, string name)
        {
            double Cost;
            double Min_cost = Double.MaxValue;
            Join best_join = null;
            foreach (String s in Join_list)
            {
                if (s.Equals("TNL"))
                {
                    Cost = TNL(LTable,RTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "TNL", selectivity, Cost, name, false);
                    }
                    Cost = TNL(RTable, LTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "TNL", selectivity, Cost, name, false);
                    }
                }

                if (s.Equals("PNL"))
                {
                    Cost = PNL(LTable, RTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "PNL", selectivity, Cost, name, false);
                    }
                    Cost = PNL(RTable, LTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "PNL", selectivity, Cost, name, false);
                    }
                }

                if (s.Equals("BNL"))
                {
                    Cost = BNL(LTable, RTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "BNL", selectivity, Cost, name, false);
                    }
                    Cost = BNL(RTable, LTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "BNL", selectivity, Cost, name, false);
                    }
                }
               
                if (s.Equals("Hash_join"))
                {
                    Cost = Hash_join(LTable, RTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "Hash_join", selectivity, Cost, name, false);
                    }
                    Cost = Hash_join(RTable, LTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "Hash_join", selectivity, Cost, name, false);
                    }
                }

                if (s.Equals("SMJ"))
                {
                    Cost = SMJ(LTable, RTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "SMJ", selectivity, Cost, name, false);
                    }
                    Cost = SMJ(RTable, LTable);
                    if (Cost < Min_cost)
                    {
                        Min_cost = Cost;
                        best_join = new Join(LTable, RTable, "SMJ", selectivity, Cost, name, false);
                    }
                }     
            }
            return best_join;
        }

        //This method do tupple neasted JOIN
        private static double TNL(Table LTable, Table RTable)
        {
            //T1.M + T1.r * T1.M * T2.N
            return
                LTable.Tuples + (LTable.Tuples * LTable.Blocks * RTable.Blocks);
        }

        //This method do Page neasted JOIN
        private static double PNL(Table LTable, Table RTable)
        {
            //T1.M + T1.M * T2.N
            return
                LTable.Tuples + (LTable.Blocks * RTable.Blocks);
        }

        private static double BNL(Table LTable, Table RTable)
        {
            return LTable.Blocks + (LTable.Blocks * RTable.Blocks);
        }

        //This method do Short Merge JOIN
        private static double SMJ(Table LTable, Table RTable)
        {
            return (LTable.Blocks + RTable.Blocks) +
                (LTable.Blocks * Math.Log(LTable.Blocks)) + (LTable.Blocks * Math.Log(LTable.Blocks));
        }

        //This method do Hash JOIN
        private static double Hash_join(Table LTable, Table RTable)
        {
            return 2 * ((LTable.Blocks / 100) + (RTable.Blocks / 100)) + 3 * (LTable.Blocks + RTable.Blocks);
        }

        //This method do Group by calculation 
        private static double GroupBy(Table table, Boolean Sorted)
        {
            if (!Sorted)
            {
                return table.Blocks * Math.Log(table.Blocks);
            }
            return table.Blocks;
        }

        //This method do Projection calculation
        private static double Projection(Table table, Boolean Sorted)
        {
            if (Sorted)
            {
                return table.Blocks;
            }
            return table.Blocks * Math.Log(table.Blocks);
        }
    }

    //Table class
    class Table
    {
        public String TableName;
        public double Tuple_Size_in_Bytes;
        public double Blocks;
        public double Tuples;
        public Boolean Sorted;

        public Table(String TableName, double Tuple_Size_in_Bytes, double Blocks, Boolean sorted)
        {
            this.TableName = TableName;
            this.Tuple_Size_in_Bytes = Tuple_Size_in_Bytes;
            this.Blocks = Blocks;
            this.Tuples = Blocks * Tuple_Size_in_Bytes;
            this.Sorted = sorted;
        }
    }

    // Disk related information 
    class Disk_info
    {

        public double Page_Size_in_Bytes = 4096;
        public double Buffer_Size_in_Bytes = 409600; 
        public double Seek_Time_in_ms = 8;
        public double Latency_in_ms = 4;
    }

    //Join Class
    class Join
    {
        public double Cost;
        public String Name;
        public String Type;
        public double Tuple_Size;
        public double Tuple_Count;
        public double selectivity;

        //Join Constructure
        public Join(Table LTable, Table RTable, String Type, String selectivity, double Cost, String Name, Boolean sorted_result)
        {
            this.Type = Type;
            this.Cost = Cost;
            this.Name = Name;
            this.Tuple_Size = LTable.Tuple_Size_in_Bytes + RTable.Tuple_Size_in_Bytes;
            this.Tuple_Count = LTable.Tuples * RTable.Tuples * Convert.ToDouble(selectivity);

        }

    }

}
