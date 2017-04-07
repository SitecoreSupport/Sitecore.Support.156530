using Sitecore.Analytics.Model;
using Sitecore.Cintel;
using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Configuration;
using Sitecore.Cintel.Search;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Analytics.Models;
using Sitecore.ContentSearch.Linq;
using Sitecore.ContentSearch.Linq.Utilities;
using Sitecore.ContentSearch.Security;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace Sitecore.Support.Cintel
{
    public class ContactSearchProvider : IContactSearchProvider
    {
        public ResultSet<List<IContactSearchResult>> Find(ContactSearchParameters parameters)
        {
            ResultSet<List<IContactSearchResult>> resultSet = new ResultSet<List<IContactSearchResult>>(parameters.PageNumber, parameters.PageSize);
            ISearchIndex index = ContentSearchManager.GetIndex(CustomerIntelligenceConfig.ContactSearch.SearchIndexName);
            using (IProviderSearchContext ctx = index.CreateSearchContext(SearchSecurityOptions.Default))
            {
                SearchResults<IndexedContact> searchResults = this.QueryIndex(ctx, parameters);
                List<IndexedContact> source = (from h in searchResults.Hits
                                               select h.Document).ToList<IndexedContact>();
                resultSet.TotalResultCount = searchResults.TotalSearchResults;
                List<IContactSearchResult> value = (from c in source.Select(delegate (IndexedContact sr)
                {
                    IContactSearchResult contact = this.BuildBaseResult(sr);
                    IndexedVisit indexedVisit = (from iv in ctx.GetQueryable<IndexedVisit>()
                                                 where iv.ContactId == contact.ContactId
                                                 orderby iv.StartDateTime descending
                                                 select iv).Take(1).FirstOrDefault<IndexedVisit>();
                    if (indexedVisit != null)
                    {
                        this.PopulateLatestVisit(indexedVisit, ref contact);
                    }
                    return contact;
                })
                                                    orderby c.FirstName, c.LatestVisitStartDateTime
                                                    select c).ToList<IContactSearchResult>();
                resultSet.Data.Dataset.Add("ContactSearchResults", value);
            }
            return resultSet;
        }

        private SearchResults<IndexedContact> QueryIndex(IProviderSearchContext ctx, ContactSearchParameters parameters)
        {
            IQueryable<IndexedContact> queryable = ctx.GetQueryable<IndexedContact>();
            string text = parameters.Match;
            if (string.IsNullOrEmpty(text.Trim()) || text == "*")
            {
                return queryable.Page(parameters.PageNumber - 1, parameters.PageSize).GetResults<IndexedContact>();
            }
            string wildcard = "*" + text + "*";
            int slop = 10;
            IQueryable<IndexedContact> source = from q in queryable
                                                where q.FullName.MatchWildcard(wildcard) || q.Emails.MatchWildcard(wildcard)
                                               select q;
           if (!source.Any<IndexedContact>())
            {
                Expression<Func<IndexedContact, bool>> emailPredicate = PredicateBuilder.True<IndexedContact>();
                Expression<Func<IndexedContact, bool>> fullNamePredicate = PredicateBuilder.True<IndexedContact>();
                foreach (string val in SplitToAtomicTerms(text))
                {
                    if (string.IsNullOrEmpty(val))
                    {
                        continue;
                    }

                    emailPredicate = emailPredicate.And(i => i.Emails.Contains(val));
                    fullNamePredicate = fullNamePredicate.And(i => i.FullName == val);                    
                }

                Expression<Func<IndexedContact, bool>> predicate = PredicateBuilder.False<IndexedContact>();
                predicate = predicate.Or(emailPredicate).Or(fullNamePredicate);
                
                source = queryable.Where(predicate);

                var rs = source.ToArray();
            }
            if (!source.Any<IndexedContact>())
            { 
                source = from q in queryable
                         where q.FullName.Like(text, slop) || q.Emails.Like(text, slop)
                         select q;
            }
            return source.Page(parameters.PageNumber - 1, parameters.PageSize).GetResults<IndexedContact>();
        }

        private IContactSearchResult BuildBaseResult(IndexedContact indexedContact)
        {
            ContactIdentificationLevel identificationLevel;
            if (!Enum.TryParse<ContactIdentificationLevel>(indexedContact.IdentificationLevel, true, out identificationLevel))
            {
                identificationLevel = ContactIdentificationLevel.None;
            }
            return new ContactSearchResult
            {
                IdentificationLevel = (int)identificationLevel,
                ContactId = indexedContact.ContactId,
                FirstName = indexedContact.FirstName,
                MiddleName = indexedContact.MiddleName,
                Surname = indexedContact.Surname,
                PreferredEmail = indexedContact.PreferredEmail,
                JobTitle = indexedContact.JobTitle,
                Value = indexedContact.Value,
                VisitCount = indexedContact.VisitCount
            };
        }

        private void PopulateLatestVisit(IndexedVisit visit, ref IContactSearchResult contact)
        {
            contact.LatestVisitId = visit.InteractionId;
            contact.LatestVisitStartDateTime = visit.StartDateTime;
            contact.LatestVisitEndDateTime = visit.EndDateTime;
            contact.LatestVisitPageViewCount = visit.VisitPageCount;
            contact.Value = visit.Value;
            if (visit.WhoIs != null)
            {
                contact.LatestVisitLocationCityDisplayName = visit.WhoIs.City;
                contact.LatestVisitLocationCountryDisplayName = visit.WhoIs.Country;
                contact.LatestVisitLocationRegionDisplayName = visit.WhoIs.Region;
                contact.LatestVisitLocationId = new Guid?(visit.LocationId);
            }
        }

        protected virtual IEnumerable<string> SplitToAtomicTerms(string query)
        {
            char[] sep = new char[1] { ' ' };
            return Regex.Replace(query, @"[^\w^\.]", sep[0].ToString()).Split(sep, StringSplitOptions.RemoveEmptyEntries);
        }
    }
}
