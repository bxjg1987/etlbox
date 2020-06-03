using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateViewTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");

        public CreateViewTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))
              , MemberData(nameof(Access))]
        public void CreateView(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateViewTask.CreateOrAlter(connection, "View1", "SELECT 1 AS test");
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View1"));
            var td = TableDefinition.FromTableName(connection, "View1");
            Assert.Contains(td.Columns, col => col.Name == "test");
        }

        [Theory, MemberData(nameof(Connections))]
        public void AlterView(IConnectionManager connection)
        {
            //Arrange
            CreateViewTask.CreateOrAlter(connection, "View2", "SELECT 1 AS Test");
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));

            //Act
            CreateViewTask.CreateOrAlter(connection, "View2", "SELECT 5 AS test");

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));
        }
    }
}
