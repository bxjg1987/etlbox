using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MergeJoinTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MergeJoinTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleMergeJoin()
        {
            //Arrange
            TwoColumnsTableFixture source1Table = new TwoColumnsTableFixture("MergeJoinSource1");
            source1Table.InsertTestData();
            TwoColumnsTableFixture source2Table = new TwoColumnsTableFixture("MergeJoinSource2");
            source2Table.InsertTestDataSet2();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture("MergeJoinDestination");

            DbSource<MySimpleRow> source1 = new DbSource<MySimpleRow>(Connection, "MergeJoinSource1");
            DbSource<MySimpleRow> source2 = new DbSource<MySimpleRow>(Connection, "MergeJoinSource2");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "MergeJoinDestination");

            //Act
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    inputRow1.Col1 += inputRow2.Col1;
                    inputRow1.Col2 += inputRow2.Col2;
                    return inputRow1;
                });
            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "MergeJoinDestination"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 5 AND Col2='Test1Test4'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 7 AND Col2='Test2Test5'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 9 AND Col2='Test3Test6'"));
        }

        [Fact]
        public void SimpleMergeJoinWithUnevenInput()
        {
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Marilyn" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "Psy" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 3, Col2 = "Adele" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 4, Col2 = "Elvis" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 5, Col2 = "James" });
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Monroe" });
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 4, Col2 = "Presley" });

            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    return new MySimpleRow()
                    {
                        Col1 = inputRow1.Col1,
                        Col2 = inputRow1.Col2 + " " + (inputRow2?.Col2 ?? "X")
                    };
                });
            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                r => Assert.True(r.Col1 == 1 && r.Col2 == "Marilyn Monroe"),
                r => Assert.True(r.Col1 == 2 && r.Col2 == "Psy Presley"),
                r => Assert.True(r.Col1 == 3 && r.Col2 == "Adele X"),
                r => Assert.True(r.Col1 == 4 && r.Col2 == "Elvis X"),
                r => Assert.True(r.Col1 == 5 && r.Col2 == "James X")
                );
        }

    }
}
