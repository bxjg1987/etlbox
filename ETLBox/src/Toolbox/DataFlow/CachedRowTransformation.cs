using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Transformations
{
    public class CachedRowTransformation<TInput, TCache, TOutput> : DataFlowTransformation<TInput, TOutput>
    {

        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Execute cached row transformation";

        /// <summary>
        /// Each ingoing row will be transformed using this Func.
        /// </summary>
        public Func<TInput, IEnumerable<TCache>, TOutput> TransformationFunc { get; set; }

        public Func<TInput, TCache> NotInCacheFunc { get; set; }
        public int MaxCacheSize { get; set; } = DEFAULT_MAX_CACHE_SIZE;

        public const int DEFAULT_MAX_CACHE_SIZE = 10000;
        /// <summary>
        /// The init action is executed shortly before the first data row is processed.
        /// </summary>
        public Action InitAction { get; set; }

        /// <inheritdoc />
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        /// <inheritdoc />
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;

        #endregion

        #region Constructors

        public CachedRowTransformation()
        {
        }

        /// <param name="transformationFunc">Will set the <see cref="TransformationFunc"/></param>
        public CachedRowTransformation(Func<TInput, IEnumerable<TCache>, TOutput> transformationFunc) : this()
        {
            TransformationFunc = transformationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TransformBlock = new TransformBlock<TInput, TOutput>(
                row =>
                {
                    NLogStartOnce();
                    try
                    {
                        InvokeInitActionOnce();
                        return InvokeTransformationFunc(row);
                    }
                    catch (Exception e)
                    {
                        ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
                        return default;
                    }
                }, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                }
            );
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        TransformBlock<TInput, TOutput> TransformBlock;
        bool WasInitActionInvoked;
        List<TCache> Cache = new List<TCache>();

        private void InvokeInitActionOnce()
        {
            if (!WasInitActionInvoked)
            {
                InitAction?.Invoke();
                WasInitActionInvoked = true;
            }
        }

        private TOutput InvokeTransformationFunc(TInput row)
        {
            TOutput result = default;

                result = TransformationFunc.Invoke(row, Cache);
                Cache.Add(row);
                if (Cache.Count > CacheSize)
                    Cache.RemoveAt(0);

            LogProgress();
            return result;
        }

        #endregion
    }

    /// <inheritdoc />
    //public class RowTransformation<TInput> : RowTransformation<TInput, TInput>
    //{
    //    public RowTransformation() : base() { }
    //    public RowTransformation(Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
    //    public RowTransformation(Func<TInput, TInput> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    //}

    /// <inheritdoc />
    //public class RowTransformation : RowTransformation<ExpandoObject>
    //{
    //    public RowTransformation() : base() { }
    //    public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
    //    public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    //}
}
